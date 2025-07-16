use futures_util::stream::StreamExt;
use std::{
    env,
    ffi::CString,
    fs,
    io::{Read, Write},
    os::unix::net::UnixStream,
    path,
};

use async_pidfd::AsyncPidFd;
use itertools::Itertools;
use nix::{
    mount::{MntFlags, MsFlags, mount, umount2},
    sched::{CloneFlags, unshare},
    sys::wait::waitpid,
    unistd::{ForkResult, chdir, execve, fork, getgid, getuid, pivot_root, sethostname},
};
use zbus::{
    proxy,
    zvariant::{OwnedObjectPath, Value},
};

pub struct ResourceLimits {
    pub memory_max: Option<u64>,        // в байтах
    pub cpu_quota_per_sec: Option<u64>, // в микросекундах
    pub pids_max: Option<u64>,          // максимальное количество процессов
}

#[proxy(
    interface = "org.freedesktop.systemd1.Manager",
    gen_blocking = false,
    default_service = "org.freedesktop.systemd1",
    default_path = "/org/freedesktop/systemd1"
)]
trait SystemdManager {
    #[zbus(name = "StartTransientUnit")]
    async fn start_transient_unit(
        &self,
        name: &str,
        mode: &str,
        properties: &[(&str, Value<'_>)],
        aux: &[(&str, &[(&str, Value<'_>)])],
    ) -> zbus::Result<OwnedObjectPath>;

    #[zbus(signal)]
    async fn job_removed(
        &self,
        id: u32,
        job: OwnedObjectPath,
        unit: String,
        result: String,
    ) -> zbus::Result<()>;
}

async fn spawn_process<'a>(
    cmd: &str,
    root_fs: &str,
    mem_max: Option<u64>,
    systemd_manager: &SystemdManagerProxy<'a>,
) -> Result<i32, Box<dyn std::error::Error>> {
    let root_fs = path::absolute(root_fs)?
        .to_str()
        .map(|p| p.to_string())
        .ok_or("Invalid root filesystem path")?;

    let (psock_wait_user_namespace, csock_wait_user_namespace) = create_sync_channel()?;
    let (psock_wait_cgroups, csock_wait_cgroups) = create_sync_channel()?;
    let (psock_grandchild_pid, csock_grandchild_pid) = create_sync_channel()?;

    match unsafe { fork()? } {
        ForkResult::Parent { child } => {
            println!("It's parent. Child pid: {}", child);

            // TODO: Make this function async
            wait_for_signal(psock_wait_user_namespace)?;
            setup_id_mapping(child.as_raw())?;

            let grandchild_pid = receive_grandchild_pid(psock_grandchild_pid)?;
            println!("Received grandchild PID: {}", grandchild_pid);

            create_cgroups_scope(
                grandchild_pid.to_string().as_str(),
                grandchild_pid,
                &ResourceLimits {
                    memory_max: mem_max,
                    cpu_quota_per_sec: None,
                    pids_max: None,
                },
                systemd_manager,
            )
            .await?;
            send_signal(&psock_wait_cgroups)?;

            let pidfd = AsyncPidFd::from_pid(child.as_raw())?;
            let status = pidfd.wait().await?;
            Ok(status.status().code().unwrap_or(-1))
        }
        ForkResult::Child => {
            println!("It's child!");

            unshare(CloneFlags::CLONE_NEWUSER)?;
            send_signal(&csock_wait_user_namespace)?;
            unshare(
                CloneFlags::CLONE_NEWPID
                    | CloneFlags::CLONE_NEWNS
                    | CloneFlags::CLONE_NEWUTS
                    | CloneFlags::CLONE_NEWIPC
                    | CloneFlags::CLONE_NEWNET,
            )?;

            match unsafe { fork()? } {
                ForkResult::Parent { child } => {
                    println!("GRANDCHILD: {}", child);
                    send_grandchild_pid(&csock_grandchild_pid, child.as_raw())?;
                    match waitpid(child, None) {
                        Ok(status) => {
                            println!("Child status: {:?}", status);
                        }
                        Err(e) => {
                            println!("Error waiting for child: {:?}", e);
                        }
                    }
                    std::process::exit(0);
                }
                ForkResult::Child => {
                    setup_filesystem(&root_fs)?;
                    sethostname("coderunner")?;

                    wait_for_signal(csock_wait_cgroups)?;
                    execute_command(cmd)?;
                }
            };

            #[allow(unreachable_code)]
            Ok(0)
        }
    }
}

async fn create_cgroups_scope<'a>(
    container_id: &str,
    pid: i32,
    limits: &ResourceLimits,
    systemd_manager: &SystemdManagerProxy<'a>,
) -> Result<String, Box<dyn std::error::Error>> {
    let scope_name = format!("container-{}.scope", container_id);

    let props = [
        Some(("Delegate", true.into())),
        Some(("PIDs", vec![pid as u32].into())),
        limits
            .memory_max
            .map(|memory_max| ("MemoryMax", memory_max.into())),
        limits
            .cpu_quota_per_sec
            .map(|cpu_quota| ("CPUQuotaPerSecUSec", cpu_quota.into())),
        limits
            .pids_max
            .map(|pids_max| ("TasksMax", pids_max.into())),
    ]
    .into_iter()
    .flatten()
    .collect::<Vec<_>>();

    let mut job_removed_stream = systemd_manager.receive_job_removed().await?;

    let job_path = systemd_manager
        .start_transient_unit(&scope_name, "fail", &props, &[])
        .await?;

    println!("✓ Запущен job для создания scope '{}'", scope_name);
    println!("  Job path: {}", job_path);

    while let Some(msg) = job_removed_stream.next().await {
        let args = msg.args()?;
        if args.unit == scope_name {
            match args.result.as_str() {
                "done" => {
                    println!(
                        "✓ Scope '{}' успешно создан и процесс перемещён",
                        scope_name
                    );
                    break;
                }
                "failed" => {
                    return Err(
                        format!("Ошибка создания scope '{}': job failed", scope_name).into(),
                    );
                }
                "timeout" => {
                    return Err(format!("Ошибка создания scope '{}': timeout", scope_name).into());
                }
                "canceled" => {
                    return Err(format!("Создание scope '{}' было отменено", scope_name).into());
                }
                _ => {
                    return Err(format!(
                        "Неожиданный результат job для scope '{}': {}",
                        scope_name, args.result
                    )
                    .into());
                }
            }
        }
    }

    Ok(scope_name)
}

fn setup_id_mapping(child_pid: i32) -> Result<(), Box<dyn std::error::Error>> {
    let uid = getuid();
    let gid = getgid();

    println!(
        "Parent: Setting up ID mapping for child {} (UID: {}, GID: {})",
        child_pid, uid, gid
    );

    let uid_map = format!("0 {} 1\n", uid.as_raw());
    let uid_map_path = format!("/proc/{}/uid_map", child_pid);
    match std::fs::OpenOptions::new().write(true).open(&uid_map_path) {
        Ok(mut file) => {
            file.write_all(uid_map.as_bytes())?;
            println!("Parent: UID mapping configured: {}", uid_map.trim());
        }
        Err(e) => {
            eprintln!("Parent: Failed to configure UID mapping: {}", e);
            return Err(e.into());
        }
    }

    let setgroups_path = format!("/proc/{}/setgroups", child_pid);
    match std::fs::OpenOptions::new()
        .write(true)
        .open(&setgroups_path)
    {
        Ok(mut file) => {
            file.write_all(b"deny")?;
            println!("Parent: setgroups configured");
        }
        Err(e) => {
            eprintln!("Parent: Failed to configure setgroups: {}", e);
            return Err(e.into());
        }
    }

    let gid_map = format!("0 {} 1\n", gid.as_raw());
    let gid_map_path = format!("/proc/{}/gid_map", child_pid);
    match std::fs::OpenOptions::new().write(true).open(&gid_map_path) {
        Ok(mut file) => {
            file.write_all(gid_map.as_bytes())?;
            println!("Parent: GID mapping configured: {}", gid_map.trim());
        }
        Err(e) => {
            eprintln!("Parent: Failed to configure GID mapping: {}", e);
            return Err(e.into());
        }
    }

    Ok(())
}

fn create_sync_channel() -> Result<(UnixStream, UnixStream), Box<dyn std::error::Error>> {
    let (parent_sock, child_sock) = UnixStream::pair()?;
    Ok((parent_sock, child_sock))
}

fn wait_for_signal(mut sock: UnixStream) -> Result<(), Box<dyn std::error::Error>> {
    let mut buf = [0u8; 1];
    sock.set_read_timeout(None)?;
    match sock.read_exact(&mut buf) {
        Ok(_) => {
            println!("Received signal from, continuing...");
            Ok(())
        }
        Err(e) => {
            eprintln!("Error reading: {}", e);
            Err(e.into())
        }
    }
}

fn send_signal(mut sock: &UnixStream) -> Result<(), Box<dyn std::error::Error>> {
    sock.write_all(&[1])?;
    println!("Signal sent");
    Ok(())
}

fn send_grandchild_pid(mut sock: &UnixStream, pid: i32) -> Result<(), Box<dyn std::error::Error>> {
    let pid_bytes = pid.to_le_bytes();
    sock.write_all(&pid_bytes)?;
    println!("Sent grandchild PID: {}", pid);
    Ok(())
}

fn receive_grandchild_pid(mut sock: UnixStream) -> Result<i32, Box<dyn std::error::Error>> {
    let mut buf = [0u8; 4]; // i32 = 4 bytes
    sock.set_read_timeout(None)?;
    sock.read_exact(&mut buf)?;
    let pid = i32::from_le_bytes(buf);
    println!("Received grandchild PID: {}", pid);
    Ok(pid)
}

fn setup_filesystem(root_fs: &str) -> Result<(), Box<dyn std::error::Error>> {
    let dirs = ["proc", "sys", "dev", "tmp"];
    for dir in &dirs {
        let path = format!("{}/{}", root_fs, dir);
        fs::create_dir_all(&path).unwrap_or(());
    }

    mount(
        None::<&str>,
        "/",
        None::<&str>,
        MsFlags::MS_REC | MsFlags::MS_PRIVATE,
        None::<&str>,
    )?;

    mount(
        Some(root_fs),
        root_fs,
        None::<&str>,
        MsFlags::MS_BIND,
        None::<&str>,
    )?;

    chdir(root_fs)?;

    let old_root = format!("{}/old_root", root_fs);
    fs::create_dir_all(&old_root).unwrap_or(());

    pivot_root(".", "old_root")?;
    chdir("/")?;

    mount(
        Some("proc"),
        "/proc",
        Some("proc"),
        MsFlags::empty(),
        None::<&str>,
    )?;

    mount(
        Some("sysfs"),
        "/sys",
        Some("sysfs"),
        MsFlags::empty(),
        None::<&str>,
    )?;

    mount(
        Some("tmpfs"),
        "/tmp",
        Some("tmpfs"),
        MsFlags::empty(),
        None::<&str>,
    )?;

    mount(
        Some("tmpfs"),
        "/dev",
        Some("tmpfs"),
        MsFlags::empty(),
        Some("mode=755"),
    )?;

    umount2("/old_root", MntFlags::MNT_DETACH)?;
    fs::remove_dir("/old_root").unwrap_or(());

    Ok(())
}

fn execute_command(cmd: &str) -> Result<(), Box<dyn std::error::Error>> {
    chdir("/")?;

    let parts: Vec<&str> = cmd.split_whitespace().collect();
    if parts.is_empty() {
        return Err("Empty command".into());
    }

    let program = CString::new(parts[0])?;
    let args: Result<Vec<CString>, _> = parts.iter().map(|&arg| CString::new(arg)).collect();
    let args = args?;

    let env = vec![
        CString::new("PATH=/usr/local/bin:/usr/bin:/bin")?,
        CString::new("HOME=/")?,
        CString::new("USER=root")?,
    ];

    println!("Executing command: {}", cmd);
    execve(&program, &args, &env)?;
    Ok(())
}

#[tokio::main]
async fn main() {
    let cmd = env::args().skip(1).join(" ");
    let mem_max = get_mem_max();

    let zbus_conn = zbus::Connection::session().await.unwrap();
    let systemd_manager = SystemdManagerProxy::new(&zbus_conn).await.unwrap();

    spawn_process(&cmd, "./rootfs", mem_max, &systemd_manager)
        .await
        .unwrap();
}

fn get_mem_max() -> Option<u64> {
    let mut mem_max = env::var("MEM_MAX").ok()?;
    let last_char = mem_max.pop()?.to_ascii_uppercase();

    let multiplier = match last_char {
        'K' => 1024,
        'M' => 1024 * 1024,
        'G' => 1024 * 1024 * 1024,
        '0'..='9' => {
            mem_max.push(last_char);
            1
        }
        _ => 1,
    };

    Some(mem_max.parse::<u64>().ok()? * multiplier)
}
