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
}

async fn spawn_process<'a>(
    cmd: &str,
    root_fs: &str,
    systemd_manager: &SystemdManagerProxy<'a>,
) -> Result<i32, Box<dyn std::error::Error>> {
    let root_fs = path::absolute(root_fs)?
        .to_str()
        .map(|p| p.to_string())
        .ok_or("Invalid root filesystem path")?;

    let (psock_wait_user_namespace, csock_wait_user_namespace) = create_sync_channel()?;
    let (psock_wait_cgroups, csock_wait_cgroups) = create_sync_channel()?;

    match unsafe { fork()? } {
        ForkResult::Parent { child } => {
            println!("It's parent. Child pid: {}", child);

            create_cgroups_scope(
                child.as_raw().to_string().as_str(),
                child.as_raw(),
                &ResourceLimits {
                    memory_max: None,
                    cpu_quota_per_sec: None,
                    pids_max: None,
                },
                systemd_manager,
            )
            .await?;
            send_signal(&psock_wait_cgroups)?;

            // TODO: Make this function async
            wait_for_signal(psock_wait_user_namespace)?;
            setup_id_mapping(child.as_raw())?;

            let pidfd = AsyncPidFd::from_pid(child.as_raw())?;
            let status = pidfd.wait().await?;
            Ok(status.status().code().unwrap_or(-1))
        }
        ForkResult::Child => {
            println!("It's child!");
            wait_for_signal(csock_wait_cgroups)?;

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

    let job_path = systemd_manager
        .start_transient_unit(&scope_name, "fail", &props, &[])
        .await?;

    println!("✓ Создан scope '{}' для контейнера", scope_name);
    println!("  Job path: {}", job_path);

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

    let zbus_conn = zbus::Connection::session().await.unwrap();
    let systemd_manager = SystemdManagerProxy::new(&zbus_conn).await.unwrap();

    spawn_process(&cmd, "./rootfs", &systemd_manager)
        .await
        .unwrap();
}
