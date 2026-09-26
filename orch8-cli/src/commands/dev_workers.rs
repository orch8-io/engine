//! `orch8 dev --worker "<cmd>"`: run local worker processes next to the
//! dev engine.
//!
//! Each `--worker` command runs through the platform shell with
//! `ORCH8_URL` / `ORCH8_API_KEY` / `ORCH8_TENANT_ID` pointing at the dev
//! server. Output lines are prefixed (`[w1 node worker.js]`) and colorized per
//! worker. A worker that exits is restarted with exponential backoff (reset
//! after it stays up for a while); `--worker-watch <glob>` restarts every
//! worker when a matching file changes.
//!
//! On Unix every worker is started in its own process group and the whole
//! group is terminated (SIGTERM, then SIGKILL after a grace period) on
//! restart and on exit, so shells that fork (`npm run`, `sh -c`) never leave
//! orphans behind. On other platforms only the direct child is killed.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime};

use anyhow::{Context, Result, bail};
use owo_colors::OwoColorize;
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::process::{Child, Command};
use tokio::sync::watch;
use tokio_util::sync::CancellationToken;

/// First restart delay after a crash.
const BACKOFF_START: Duration = Duration::from_millis(500);
/// Restart delay cap.
const BACKOFF_MAX: Duration = Duration::from_secs(30);
/// A worker that ran at least this long resets its backoff.
const STABLE_AFTER: Duration = Duration::from_secs(30);
/// Grace period between SIGTERM and SIGKILL.
const KILL_GRACE: Duration = Duration::from_secs(5);
/// How often `--worker-watch` rescans.
const WATCH_POLL: Duration = Duration::from_millis(500);
/// Directories never descended into by the watcher.
const SKIP_DIRS: &[&str] = &[
    ".git",
    "target",
    "node_modules",
    ".orch8",
    ".venv",
    "__pycache__",
];

/// Environment handed to every worker.
#[derive(Debug, Clone)]
pub struct WorkerEnv {
    pub url: String,
    pub api_key: Option<String>,
    pub tenant_id: String,
}

impl WorkerEnv {
    fn apply(&self, command: &mut Command) {
        command.env("ORCH8_URL", &self.url);
        command.env("ORCH8_TENANT_ID", &self.tenant_id);
        command.env("ORCH8_API_KEY", self.api_key.as_deref().unwrap_or_default());
    }
}

/// Exponential restart backoff with a stability reset.
#[derive(Debug, Clone)]
pub struct Backoff {
    next: Duration,
}

impl Default for Backoff {
    fn default() -> Self {
        Self {
            next: BACKOFF_START,
        }
    }
}

impl Backoff {
    /// Delay before the next restart, given how long the last run lasted.
    pub fn next_delay(&mut self, ran_for: Duration) -> Duration {
        if ran_for >= STABLE_AFTER {
            self.next = BACKOFF_START;
        }
        let delay = self.next;
        self.next = (self.next * 2).min(BACKOFF_MAX);
        delay
    }
}

/// Short label for a worker command: `w1 node worker.js`.
pub fn worker_label(index: usize, command: &str) -> String {
    let short: String = command.chars().take(24).collect();
    let short = if command.chars().count() > 24 {
        format!("{}…", short.trim_end())
    } else {
        short
    };
    format!("w{} {short}", index + 1)
}

fn paint(index: usize, text: &str) -> String {
    match index % 6 {
        0 => text.cyan().to_string(),
        1 => text.magenta().to_string(),
        2 => text.green().to_string(),
        3 => text.yellow().to_string(),
        4 => text.blue().to_string(),
        _ => text.bright_red().to_string(),
    }
}

fn shell_command(command: &str) -> Command {
    #[cfg(unix)]
    {
        let mut cmd = Command::new("/bin/sh");
        cmd.arg("-c").arg(command);
        // New process group (pgid = child pid) so the whole tree can be
        // signalled at once and terminal ^C doesn't race our shutdown.
        cmd.process_group(0);
        cmd
    }
    #[cfg(not(unix))]
    {
        let mut cmd = Command::new("cmd");
        cmd.arg("/C").arg(command);
        cmd
    }
}

/// Terminate a worker and everything it spawned.
async fn terminate(child: &mut Child) {
    #[cfg(unix)]
    if let Some(pid) = child.id() {
        use nix::sys::signal::{Signal, killpg};
        use nix::unistd::Pid;
        let pgid = Pid::from_raw(i32::try_from(pid).unwrap_or(i32::MAX));
        let _ = killpg(pgid, Signal::SIGTERM);
        if tokio::time::timeout(KILL_GRACE, child.wait()).await.is_ok() {
            // Leader exited; sweep any stragglers left in the group.
            let _ = killpg(pgid, Signal::SIGKILL);
            return;
        }
        let _ = killpg(pgid, Signal::SIGKILL);
    }
    let _ = child.kill().await;
}

/// Synchronous last-resort cleanup (panics / early returns).
#[cfg(unix)]
fn kill_group_now(pid: u32) {
    use nix::sys::signal::{Signal, killpg};
    use nix::unistd::Pid;
    let _ = killpg(
        Pid::from_raw(i32::try_from(pid).unwrap_or(i32::MAX)),
        Signal::SIGKILL,
    );
}

/// Owns the supervisor tasks; dropping it (or calling [`Self::shutdown`])
/// stops every worker group.
pub struct WorkerSupervisor {
    cancel: CancellationToken,
    tasks: Vec<tokio::task::JoinHandle<()>>,
    pids: Arc<std::sync::Mutex<HashMap<usize, u32>>>,
}

impl WorkerSupervisor {
    /// Start one supervised process per command. `watch` globs trigger a
    /// restart of every worker when a matching file changes.
    pub fn start(commands: &[String], env: &WorkerEnv, watch_globs: &[String]) -> Result<Self> {
        let cancel = CancellationToken::new();
        let (restart_tx, restart_rx) = watch::channel(0_u64);
        let pids = Arc::new(std::sync::Mutex::new(HashMap::new()));
        let mut tasks = Vec::new();
        if !watch_globs.is_empty() {
            let watcher = GlobWatch::new(watch_globs)?;
            let cancel = cancel.clone();
            tasks.push(tokio::spawn(watch_loop(watcher, restart_tx, cancel)));
        }
        for (index, command) in commands.iter().enumerate() {
            let label = worker_label(index, command);
            eprintln!(
                "{} {} {}",
                "worker:".bold(),
                paint(index, &format!("[{label}]")),
                command.dimmed()
            );
            tasks.push(tokio::spawn(supervise(
                index,
                command.clone(),
                env.clone(),
                cancel.clone(),
                restart_rx.clone(),
                Arc::clone(&pids),
            )));
        }
        Ok(Self {
            cancel,
            tasks,
            pids,
        })
    }

    /// Stop every worker (process groups included) and wait for the
    /// supervisors to finish.
    pub async fn shutdown(mut self) {
        self.cancel.cancel();
        for task in self.tasks.drain(..) {
            let _ = task.await;
        }
    }
}

impl Drop for WorkerSupervisor {
    fn drop(&mut self) {
        self.cancel.cancel();
        #[cfg(unix)]
        if let Ok(pids) = self.pids.lock() {
            for pid in pids.values() {
                kill_group_now(*pid);
            }
        }
    }
}

async fn supervise(
    index: usize,
    command: String,
    env: WorkerEnv,
    cancel: CancellationToken,
    mut restart: watch::Receiver<u64>,
    pids: Arc<std::sync::Mutex<HashMap<usize, u32>>>,
) {
    let label = paint(index, &format!("[{}]", worker_label(index, &command)));
    let mut backoff = Backoff::default();
    restart.mark_unchanged();
    loop {
        let mut cmd = shell_command(&command);
        env.apply(&mut cmd);
        cmd.stdin(std::process::Stdio::null())
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped())
            .kill_on_drop(true);
        let started = Instant::now();
        let mut child = match cmd.spawn() {
            Ok(child) => child,
            Err(error) => {
                eprintln!("{label} {} failed to start: {error}", "error:".red().bold());
                let delay = backoff.next_delay(Duration::ZERO);
                tokio::select! {
                    () = cancel.cancelled() => return,
                    () = tokio::time::sleep(delay) => continue,
                }
            }
        };
        // Capture the pid now: tokio clears it once the child is reaped.
        let pid = child.id();
        if let (Some(pid), Ok(mut map)) = (pid, pids.lock()) {
            map.insert(index, pid);
        }
        if let Some(stdout) = child.stdout.take() {
            tokio::spawn(pump(BufReader::new(stdout), label.clone(), false));
        }
        if let Some(stderr) = child.stderr.take() {
            tokio::spawn(pump(BufReader::new(stderr), label.clone(), true));
        }

        let outcome = tokio::select! {
            status = child.wait() => Some(status),
            () = cancel.cancelled() => None,
            changed = restart.changed() => {
                if changed.is_err() {
                    // Watcher gone; keep running until exit or cancel.
                    tokio::select! {
                        status = child.wait() => Some(status),
                        () = cancel.cancelled() => None,
                    }
                } else {
                    eprintln!("{label} {} file change detected — restarting", "↻".cyan());
                    terminate(&mut child).await;
                    backoff = Backoff::default();
                    if let Ok(mut map) = pids.lock() {
                        map.remove(&index);
                    }
                    continue;
                }
            }
        };
        let Some(status) = outcome else {
            terminate(&mut child).await;
            if let Ok(mut map) = pids.lock() {
                map.remove(&index);
            }
            return;
        };
        if let Ok(mut map) = pids.lock() {
            map.remove(&index);
        }
        // Also reap anything the leader left in its group.
        #[cfg(unix)]
        if let Some(pid) = pid {
            kill_group_now(pid);
        }
        #[cfg(not(unix))]
        let _ = pid;
        let delay = backoff.next_delay(started.elapsed());
        match status {
            Ok(status) => eprintln!(
                "{label} {} exited ({status}); restarting in {:.1}s",
                "worker".yellow(),
                delay.as_secs_f64()
            ),
            Err(error) => eprintln!(
                "{label} {} wait failed ({error}); restarting in {:.1}s",
                "error:".red().bold(),
                delay.as_secs_f64()
            ),
        }
        tokio::select! {
            () = cancel.cancelled() => return,
            () = tokio::time::sleep(delay) => {}
        }
    }
}

async fn pump<R: tokio::io::AsyncRead + Unpin>(reader: BufReader<R>, label: String, stderr: bool) {
    let mut lines = reader.lines();
    while let Ok(Some(line)) = lines.next_line().await {
        if stderr {
            eprintln!("{label} {line}");
        } else {
            println!("{label} {line}");
        }
    }
}

async fn watch_loop(
    mut watcher: GlobWatch,
    restart: watch::Sender<u64>,
    cancel: CancellationToken,
) {
    let mut generation = 0_u64;
    loop {
        tokio::select! {
            () = cancel.cancelled() => return,
            () = tokio::time::sleep(WATCH_POLL) => {}
        }
        let changed = watcher.poll();
        if !changed.is_empty() {
            generation += 1;
            eprintln!(
                "{} {} changed",
                "worker-watch:".bold(),
                changed
                    .iter()
                    .take(3)
                    .map(|p| p.display().to_string())
                    .collect::<Vec<_>>()
                    .join(", ")
            );
            let _ = restart.send(generation);
        }
    }
}

/// Poll-based glob watcher: (mtime, size) signatures of every matching file
/// under each glob's literal base directory.
pub struct GlobWatch {
    roots: Vec<PathBuf>,
    set: globset::GlobSet,
    signatures: HashMap<PathBuf, (SystemTime, u64)>,
}

/// The literal directory prefix of a glob (`src/**/*.ts` → `src`).
fn glob_root(pattern: &str) -> PathBuf {
    let mut root = PathBuf::new();
    for component in Path::new(pattern).components() {
        let text = component.as_os_str().to_string_lossy();
        if text.contains(['*', '?', '[', '{']) {
            break;
        }
        root.push(component);
    }
    if root.as_os_str().is_empty() || root == Path::new(pattern) && !root.is_dir() {
        root.parent()
            .filter(|p| !p.as_os_str().is_empty())
            .map_or_else(|| PathBuf::from("."), Path::to_path_buf)
    } else {
        root
    }
}

impl GlobWatch {
    pub fn new(patterns: &[String]) -> Result<Self> {
        let mut builder = globset::GlobSetBuilder::new();
        let mut roots = Vec::new();
        for pattern in patterns {
            let normalized = pattern.strip_prefix("./").unwrap_or(pattern);
            builder.add(
                globset::GlobBuilder::new(normalized)
                    .literal_separator(true)
                    .build()
                    .with_context(|| format!("invalid --worker-watch glob '{pattern}'"))?,
            );
            let root = glob_root(normalized);
            if !roots.contains(&root) {
                roots.push(root);
            }
        }
        let set = builder.build().context("invalid --worker-watch globs")?;
        if patterns.is_empty() {
            bail!("no --worker-watch globs");
        }
        let mut watch = Self {
            roots,
            set,
            signatures: HashMap::new(),
        };
        watch.signatures = watch.scan();
        Ok(watch)
    }

    fn scan(&self) -> HashMap<PathBuf, (SystemTime, u64)> {
        let mut out = HashMap::new();
        for root in &self.roots {
            self.walk(root, &mut out, 0);
        }
        out
    }

    fn walk(&self, dir: &Path, out: &mut HashMap<PathBuf, (SystemTime, u64)>, depth: usize) {
        if depth > 32 {
            return;
        }
        if dir.is_file() {
            self.consider(dir, out);
            return;
        }
        let Ok(entries) = std::fs::read_dir(dir) else {
            return;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            let Ok(kind) = entry.file_type() else {
                continue;
            };
            if kind.is_dir() {
                let name = entry.file_name();
                if SKIP_DIRS.iter().any(|skip| name == *skip) {
                    continue;
                }
                self.walk(&path, out, depth + 1);
            } else if kind.is_file() {
                self.consider(&path, out);
            }
        }
    }

    fn consider(&self, path: &Path, out: &mut HashMap<PathBuf, (SystemTime, u64)>) {
        let relative = path.strip_prefix(".").unwrap_or(path);
        if !self.set.is_match(relative) {
            return;
        }
        if let Ok(meta) = std::fs::metadata(path) {
            out.insert(
                path.to_path_buf(),
                (
                    meta.modified().unwrap_or(SystemTime::UNIX_EPOCH),
                    meta.len(),
                ),
            );
        }
    }

    /// Paths added, changed, or removed since the last poll.
    pub fn poll(&mut self) -> Vec<PathBuf> {
        let current = self.scan();
        let mut changed: Vec<PathBuf> = current
            .iter()
            .filter(|(path, sig)| self.signatures.get(*path) != Some(sig))
            .map(|(path, _)| path.clone())
            .collect();
        changed.extend(
            self.signatures
                .keys()
                .filter(|path| !current.contains_key(*path))
                .cloned(),
        );
        changed.sort();
        self.signatures = current;
        changed
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn backoff_doubles_caps_and_resets_after_stable_run() {
        let mut b = Backoff::default();
        assert_eq!(b.next_delay(Duration::ZERO), Duration::from_millis(500));
        assert_eq!(b.next_delay(Duration::ZERO), Duration::from_secs(1));
        assert_eq!(b.next_delay(Duration::ZERO), Duration::from_secs(2));
        for _ in 0..10 {
            b.next_delay(Duration::ZERO);
        }
        assert_eq!(b.next_delay(Duration::ZERO), BACKOFF_MAX);
        assert_eq!(b.next_delay(STABLE_AFTER), BACKOFF_START);
    }

    #[test]
    fn labels_are_short_and_numbered() {
        assert_eq!(worker_label(0, "node worker.js"), "w1 node worker.js");
        let long = worker_label(2, "python -m my_company.workers.billing --verbose");
        assert!(long.starts_with("w3 python -m my_company.w"), "{long}");
        assert!(long.ends_with('…'));
    }

    #[test]
    fn glob_roots_are_literal_prefixes() {
        assert_eq!(glob_root("src/**/*.ts"), PathBuf::from("src"));
        assert_eq!(glob_root("*.py"), PathBuf::from("."));
        assert_eq!(
            glob_root("workers/billing/*.js"),
            PathBuf::from("workers/billing")
        );
    }

    #[test]
    fn glob_watch_detects_add_change_and_remove() {
        let dir = tempfile::tempdir().unwrap();
        let src = dir.path().join("src");
        std::fs::create_dir_all(src.join("nested")).unwrap();
        std::fs::create_dir_all(src.join("node_modules")).unwrap();
        std::fs::write(src.join("a.ts"), "1").unwrap();
        std::fs::write(src.join("node_modules/ignored.ts"), "1").unwrap();
        let pattern = format!("{}/**/*.ts", src.display());
        let mut watch = GlobWatch::new(&[pattern]).unwrap();
        assert!(watch.poll().is_empty());

        std::fs::write(src.join("nested/b.ts"), "2").unwrap();
        std::fs::write(src.join("notes.md"), "x").unwrap();
        assert_eq!(watch.poll(), vec![src.join("nested/b.ts")]);

        std::fs::write(src.join("a.ts"), "longer").unwrap();
        assert_eq!(watch.poll(), vec![src.join("a.ts")]);

        std::fs::remove_file(src.join("nested/b.ts")).unwrap();
        assert_eq!(watch.poll(), vec![src.join("nested/b.ts")]);

        std::fs::write(src.join("node_modules/ignored.ts"), "changed!").unwrap();
        assert!(watch.poll().is_empty());
    }

    #[cfg(unix)]
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn workers_get_env_restart_on_crash_and_leave_no_orphans() {
        let dir = tempfile::tempdir().unwrap();
        let marker = dir.path().join("runs");
        let grandchild_pid = dir.path().join("grandchild.pid");
        // Each run appends the env it saw, spawns a long-lived grandchild in
        // the same process group, and exits 1 (a crash) on the first run.
        let script = format!(
            "echo \"$ORCH8_URL|$ORCH8_TENANT_ID|$ORCH8_API_KEY\" >> {marker}; \
             sleep 300 & echo $! > {pid}; \
             if [ $(wc -l < {marker}) -lt 2 ]; then exit 1; fi; wait",
            marker = marker.display(),
            pid = grandchild_pid.display()
        );
        let env = WorkerEnv {
            url: "http://127.0.0.1:9/api/v1".into(),
            api_key: Some("k".into()),
            tenant_id: "t".into(),
        };
        let supervisor = WorkerSupervisor::start(&[script], &env, &[]).unwrap();

        // Wait for the restart (second run).
        let deadline = Instant::now() + Duration::from_secs(10);
        loop {
            let runs = std::fs::read_to_string(&marker).unwrap_or_default();
            if runs.lines().count() >= 2 {
                assert_eq!(
                    runs.lines().next().unwrap(),
                    "http://127.0.0.1:9/api/v1|t|k"
                );
                break;
            }
            assert!(
                Instant::now() < deadline,
                "worker was not restarted: {runs:?}"
            );
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        // Let the second run record its grandchild.
        tokio::time::sleep(Duration::from_millis(300)).await;
        let pid: i32 = std::fs::read_to_string(&grandchild_pid)
            .unwrap()
            .trim()
            .parse()
            .unwrap();
        supervisor.shutdown().await;

        // The grandchild (same process group) must be gone.
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            let alive = nix::sys::signal::kill(nix::unistd::Pid::from_raw(pid), None).is_ok();
            if !alive {
                break;
            }
            assert!(
                Instant::now() < deadline,
                "grandchild {pid} survived shutdown"
            );
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }
}
