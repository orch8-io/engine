//! `orch8 learn` — an interactive terminal walk through the progressive
//! quick starts in `docs/quick-starts/`.
//!
//! The guides are embedded at compile time, so the tutorial and the docs can
//! never drift apart: every `## N. Title` section of every level becomes one
//! step. Steps with an automated check run real commands — the current
//! `orch8` binary executes `orch8 dev --once` against a local, ephemeral dev
//! engine, or starts a local dev server and publishes to it — and verify the
//! result. The remaining steps are shown with their commands for you to run,
//! then marked done.
//!
//! Progress lives in `.orch8/learn.json`; tutorial files in
//! `.orch8/learn/level-N/`. Without a TTY the command just prints the
//! current (or `--step N`) step, so it is safe in scripts and CI.

use std::io::{IsTerminal as _, Write as _};
use std::path::{Path, PathBuf};
use std::time::Duration;

use anyhow::{Context, Result, bail};
use clap::Args;
use owo_colors::OwoColorize;
use serde::{Deserialize, Serialize};

#[derive(Debug, Args)]
pub struct LearnCmd {
    /// List every step with its completion status.
    #[arg(long)]
    pub list: bool,
    /// Jump to step N (1-based, as shown by `--list`).
    #[arg(long, value_name = "N")]
    pub step: Option<usize>,
    /// Forget all progress (and the tutorial workspace) and start over.
    #[arg(long)]
    pub reset: bool,
    /// Run the current (or `--step N`) step's automated check without
    /// prompting — works without a TTY.
    #[arg(long)]
    pub check: bool,
    /// Project directory holding `.orch8/learn.json`.
    #[arg(long, default_value = ".")]
    pub dir: PathBuf,
}

/// One embedded quick-start level.
struct Level {
    number: u8,
    doc: &'static str,
    markdown: &'static str,
}

const LEVELS: &[Level] = &[
    Level {
        number: 1,
        doc: "docs/quick-starts/01-local-workflow.md",
        markdown: include_str!("../../../docs/quick-starts/01-local-workflow.md"),
    },
    Level {
        number: 2,
        doc: "docs/quick-starts/02-data-and-routing.md",
        markdown: include_str!("../../../docs/quick-starts/02-data-and-routing.md"),
    },
    Level {
        number: 3,
        doc: "docs/quick-starts/03-durable-api.md",
        markdown: include_str!("../../../docs/quick-starts/03-durable-api.md"),
    },
    Level {
        number: 4,
        doc: "docs/quick-starts/04-external-worker.md",
        markdown: include_str!("../../../docs/quick-starts/04-external-worker.md"),
    },
    Level {
        number: 5,
        doc: "docs/quick-starts/05-failure-and-recovery.md",
        markdown: include_str!("../../../docs/quick-starts/05-failure-and-recovery.md"),
    },
    Level {
        number: 6,
        doc: "docs/quick-starts/06-production-release.md",
        markdown: include_str!("../../../docs/quick-starts/06-production-release.md"),
    },
];

/// A fenced code block from a guide.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CodeBlock {
    pub lang: String,
    pub code: String,
}

/// One tutorial step (a numbered section of a level).
#[derive(Debug, Clone)]
pub struct Step {
    /// `level.section`, e.g. `1.2`.
    pub id: String,
    pub level: u8,
    pub section: u32,
    pub level_title: String,
    pub title: String,
    /// Prose with code blocks removed.
    pub prose: String,
    pub code: Vec<CodeBlock>,
    pub doc: &'static str,
}

/// Parse every embedded level into steps, in order.
pub fn steps() -> Vec<Step> {
    LEVELS.iter().flat_map(parse_level).collect()
}

fn parse_level(level: &Level) -> Vec<Step> {
    let mut level_title = String::new();
    let mut out: Vec<Step> = Vec::new();
    let mut in_code: Option<CodeBlock> = None;
    for line in level.markdown.lines() {
        if let Some(block) = in_code.as_mut() {
            if line.trim_start().starts_with("```") {
                let block = in_code.take().expect("inside a code block");
                // Code before the first numbered section is level intro.
                if let Some(step) = out.last_mut() {
                    step.code.push(block);
                }
            } else {
                block.code.push_str(line);
                block.code.push('\n');
            }
            continue;
        }
        if let Some(fence) = line.trim_start().strip_prefix("```") {
            in_code = Some(CodeBlock {
                lang: fence.trim().to_string(),
                code: String::new(),
            });
            if let Some(step) = out.last_mut() {
                step.prose.push_str("  [");
                step.prose.push_str(code_label(fence.trim()));
                step.prose.push_str("]\n");
            }
            continue;
        }
        if let Some(title) = line.strip_prefix("# ") {
            level_title = title.trim().to_string();
            continue;
        }
        if let Some(heading) = line.strip_prefix("## ") {
            // Numbered sections are steps; "Checkpoint" / "If something
            // failed" close the level.
            let numbered = heading
                .split_once(". ")
                .and_then(|(n, t)| n.trim().parse::<u32>().ok().map(|n| (n, t.trim())));
            match numbered {
                Some((section, title)) => out.push(Step {
                    id: format!("{}.{section}", level.number),
                    level: level.number,
                    section,
                    level_title: level_title.clone(),
                    title: title.to_string(),
                    prose: String::new(),
                    code: Vec::new(),
                    doc: level.doc,
                }),
                None => break,
            }
            continue;
        }
        if let Some(step) = out.last_mut() {
            step.prose.push_str(line);
            step.prose.push('\n');
        }
    }
    for step in &mut out {
        step.prose = step.prose.trim().to_string();
    }
    out
}

fn code_label(lang: &str) -> &'static str {
    match lang {
        "json" => "JSON shown below",
        "bash" | "sh" | "shell" => "commands shown below",
        _ => "snippet shown below",
    }
}

/// Automated check for a step, when one exists.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Check {
    /// Save the step's JSON code block as `level-N/sequence.json`.
    WriteSequence { level: u8 },
    /// `orch8 dev level-N --no-server --once [--skip-timers] [--input …]`
    /// must exit 0 and print `expect`.
    DevOnce {
        level: u8,
        input: Option<&'static str>,
        skip_timers: bool,
        expect: &'static str,
    },
    /// A duplicate-block-id copy must be rejected before execution.
    DevRejectsDuplicateIds { level: u8 },
    /// Start a local dev engine with its HTTP API, preflight and publish the
    /// Level 2 sequence to it with `orch8 sequence`, then stop it.
    PublishToDevEngine,
}

/// Map a step to its automated check.
pub fn check_for(step: &Step) -> Option<Check> {
    Some(match step.id.as_str() {
        "1.1" => Check::WriteSequence { level: 1 },
        "1.2" => Check::DevOnce {
            level: 1,
            input: None,
            skip_timers: true,
            expect: "instance completed",
        },
        "1.4" => Check::DevRejectsDuplicateIds { level: 1 },
        "2.1" => Check::WriteSequence { level: 2 },
        "2.2" => Check::DevOnce {
            level: 2,
            input: Some(r#"{"customer":"Ada","plan":"paid"}"#),
            skip_timers: false,
            expect: "paid_welcome",
        },
        "2.3" => Check::DevOnce {
            level: 2,
            input: Some(r#"{"customer":"Linus","plan":"trial"}"#),
            skip_timers: false,
            expect: "trial_welcome",
        },
        "3.4" => Check::PublishToDevEngine,
        _ => return None,
    })
}

// ---------------------------------------------------------------------------
// Progress
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct Progress {
    #[serde(default)]
    pub completed: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub current: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub updated_at: Option<String>,
}

impl Progress {
    fn path(dir: &Path) -> PathBuf {
        dir.join(".orch8").join("learn.json")
    }

    pub fn load(dir: &Path) -> Result<Self> {
        match std::fs::read(Self::path(dir)) {
            Ok(bytes) => serde_json::from_slice(&bytes).with_context(|| {
                format!(
                    "{} is corrupt; run `orch8 learn --reset`",
                    Self::path(dir).display()
                )
            }),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(Self::default()),
            Err(e) => Err(e).context("read learn progress"),
        }
    }

    pub fn save(&mut self, dir: &Path) -> Result<()> {
        let path = Self::path(dir);
        std::fs::create_dir_all(path.parent().unwrap_or(dir))?;
        self.updated_at = Some(chrono::Utc::now().to_rfc3339());
        crate::atomic_write(
            &path,
            format!("{}\n", serde_json::to_string_pretty(self)?).as_bytes(),
        )
    }

    pub fn is_done(&self, id: &str) -> bool {
        self.completed.iter().any(|c| c == id)
    }

    pub fn complete(&mut self, id: &str) {
        if !self.is_done(id) {
            self.completed.push(id.to_string());
        }
    }
}

/// Index of the first incomplete step (or the last step when all are done).
fn next_index(steps: &[Step], progress: &Progress) -> usize {
    steps
        .iter()
        .position(|s| !progress.is_done(&s.id))
        .unwrap_or(steps.len().saturating_sub(1))
}

fn workspace(dir: &Path, level: u8) -> PathBuf {
    dir.join(".orch8")
        .join("learn")
        .join(format!("level-{level}"))
}

// ---------------------------------------------------------------------------
// Rendering
// ---------------------------------------------------------------------------

/// Plain-text rendering of a step (used for TTY and non-TTY output).
pub fn render_step(step: &Step, index: usize, total: usize, dir: &Path) -> String {
    use std::fmt::Write as _;
    let mut out = String::new();
    let _ = writeln!(
        out,
        "{} {}  {}",
        format!("Step {}/{total}", index + 1).bold(),
        format!("({})", step.id).dimmed(),
        step.level_title.dimmed()
    );
    let _ = writeln!(
        out,
        "{}\n",
        format!("{}. {}", step.section, step.title).bold().cyan()
    );
    let _ = writeln!(out, "{}\n", step.prose);
    for block in &step.code {
        let _ = writeln!(
            out,
            "{}",
            format!(
                "── {} ──",
                if block.lang.is_empty() {
                    "text"
                } else {
                    &block.lang
                }
            )
            .dimmed()
        );
        for line in block.code.lines() {
            let _ = writeln!(out, "  {line}");
        }
        out.push('\n');
    }
    match check_for(step) {
        Some(check) => {
            let _ = writeln!(
                out,
                "{} {}",
                "Automated check:".green().bold(),
                describe_check(&check, dir)
            );
        }
        None => {
            let _ = writeln!(
                out,
                "{} run the commands above yourself (the guide explains what to look for), \
                 then mark the step done.",
                "Manual step:".yellow().bold()
            );
        }
    }
    let _ = writeln!(out, "{} {}", "Guide:".dimmed(), step.doc);
    out
}

fn describe_check(check: &Check, dir: &Path) -> String {
    match check {
        Check::WriteSequence { level } => format!(
            "writes the JSON above to {}/sequence.json and validates it",
            workspace(dir, *level).display()
        ),
        Check::DevOnce { level, input, .. } => format!(
            "runs `orch8 dev {} --no-server --once{}` on a local dev engine",
            workspace(dir, *level).display(),
            input.map(|i| format!(" --input '{i}'")).unwrap_or_default()
        ),
        Check::DevRejectsDuplicateIds { .. } => {
            "runs `orch8 dev` on a copy with a duplicate block id and expects a rejection before execution".into()
        }
        Check::PublishToDevEngine => "starts a local dev engine with its HTTP API, runs `orch8 sequence preflight` and `orch8 sequence create` against it, then stops it".into(),
    }
}

pub fn render_list(steps: &[Step], progress: &Progress) -> String {
    use std::fmt::Write as _;
    let mut out = String::new();
    let mut level = 0;
    for (index, step) in steps.iter().enumerate() {
        if step.level != level {
            level = step.level;
            let _ = writeln!(out, "\n{}", step.level_title.bold());
        }
        let mark = if progress.is_done(&step.id) {
            "✓".green().to_string()
        } else {
            "·".dimmed().to_string()
        };
        let auto = if check_for(step).is_some() {
            " [auto]"
        } else {
            ""
        };
        let _ = writeln!(
            out,
            "  {mark} {:>2}. {} {}{}",
            index + 1,
            step.title,
            format!("({})", step.id).dimmed(),
            auto.dimmed()
        );
    }
    let done = steps.iter().filter(|s| progress.is_done(&s.id)).count();
    let _ = writeln!(out, "\n{done}/{} steps complete", steps.len());
    out
}

// ---------------------------------------------------------------------------
// Checks
// ---------------------------------------------------------------------------

/// Result of running a check.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CheckOutcome {
    pub passed: bool,
    pub detail: String,
}

fn orch8_binary() -> Result<PathBuf> {
    if let Some(bin) = std::env::var_os("ORCH8_LEARN_BIN") {
        return Ok(PathBuf::from(bin));
    }
    std::env::current_exe().context("locate the orch8 binary")
}

async fn run_orch8(args: &[String], cwd: &Path, timeout: Duration) -> Result<(bool, String)> {
    let mut command = tokio::process::Command::new(orch8_binary()?);
    command
        .args(args)
        .current_dir(cwd)
        .env("NO_COLOR", "1")
        .stdin(std::process::Stdio::null())
        .kill_on_drop(true);
    let output = tokio::time::timeout(timeout, command.output())
        .await
        .context("command timed out")?
        .context("run orch8")?;
    let mut text = String::from_utf8_lossy(&output.stdout).into_owned();
    text.push_str(&String::from_utf8_lossy(&output.stderr));
    Ok((output.status.success(), text))
}

fn tail(text: &str, lines: usize) -> String {
    let all: Vec<&str> = text.lines().collect();
    all[all.len().saturating_sub(lines)..].join("\n")
}

/// Run a step's check against `dir` (the project directory).
pub async fn run_check(step: &Step, check: &Check, dir: &Path) -> Result<CheckOutcome> {
    match check {
        Check::WriteSequence { level } => {
            let block = step
                .code
                .iter()
                .find(|b| b.lang == "json")
                .context("the guide step has no JSON block")?;
            let value: serde_json::Value =
                serde_json::from_str(&block.code).context("the guide's JSON does not parse")?;
            let ws = workspace(dir, *level);
            std::fs::create_dir_all(&ws)?;
            let path = ws.join("sequence.json");
            crate::atomic_write(&path, block.code.as_bytes())?;
            // Validate exactly as `orch8 dev` will.
            super::dev::parse_sequence_value(value, 1)?;
            Ok(CheckOutcome {
                passed: true,
                detail: format!("wrote and validated {}", path.display()),
            })
        }
        Check::DevOnce {
            level,
            input,
            skip_timers,
            expect,
        } => {
            let ws = workspace(dir, *level);
            if !ws.join("sequence.json").is_file() {
                bail!(
                    "{} is missing — complete step {}.1 first",
                    ws.join("sequence.json").display(),
                    level
                );
            }
            let mut args = vec![
                "dev".to_string(),
                ws.display().to_string(),
                "--no-server".into(),
                "--once".into(),
            ];
            if *skip_timers {
                args.push("--skip-timers".into());
            }
            if let Some(input) = input {
                args.push("--input".into());
                args.push((*input).to_string());
            }
            let (ok, output) = run_orch8(&args, dir, Duration::from_secs(120)).await?;
            let passed = ok && output.contains(expect);
            Ok(CheckOutcome {
                passed,
                detail: if passed {
                    format!(
                        "`orch8 {}` completed and printed '{expect}'",
                        args.join(" ")
                    )
                } else {
                    format!(
                        "expected success and '{expect}'; got:\n{}",
                        tail(&output, 12)
                    )
                },
            })
        }
        Check::DevRejectsDuplicateIds { level } => {
            let ws = workspace(dir, *level);
            let source = std::fs::read_to_string(ws.join("sequence.json")).with_context(|| {
                format!(
                    "{} is missing — complete step {level}.1 first",
                    ws.join("sequence.json").display()
                )
            })?;
            let invalid = source.replace("\"id\": \"finish\"", "\"id\": \"greet\"");
            let path = ws.join("invalid-sequence.json");
            std::fs::write(&path, invalid)?;
            let args = vec![
                "dev".to_string(),
                path.display().to_string(),
                "--no-server".into(),
                "--once".into(),
            ];
            let result = run_orch8(&args, dir, Duration::from_secs(60)).await;
            let _ = std::fs::remove_file(&path);
            let (ok, output) = result?;
            let passed = !ok && output.contains("duplicate block id");
            Ok(CheckOutcome {
                passed,
                detail: if passed {
                    "the duplicate block id was rejected before any step ran".into()
                } else {
                    format!(
                        "expected a 'duplicate block id' rejection; got:\n{}",
                        tail(&output, 12)
                    )
                },
            })
        }
        Check::PublishToDevEngine => publish_to_dev_engine(dir).await,
    }
}

async fn publish_to_dev_engine(dir: &Path) -> Result<CheckOutcome> {
    let level2 = workspace(dir, 2).join("sequence.json");
    if !level2.is_file() {
        bail!("{} is missing — complete step 2.1 first", level2.display());
    }
    let level1 = workspace(dir, 1);
    // A fresh dev database each time, so re-running the check never hits
    // "already exists" for the guide's fixed sequence id.
    for suffix in ["dev.db", "dev.db-wal", "dev.db-shm"] {
        let _ = std::fs::remove_file(level1.join(".orch8").join(suffix));
    }
    let port = std::net::TcpListener::bind("127.0.0.1:0")?
        .local_addr()?
        .port();
    let mut server = tokio::process::Command::new(orch8_binary()?)
        .args([
            "dev".to_string(),
            level1.display().to_string(),
            "--skip-timers".into(),
            "--port".into(),
            port.to_string(),
        ])
        .current_dir(dir)
        .env("NO_COLOR", "1")
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .kill_on_drop(true)
        .spawn()
        .context("start the local dev engine")?;
    let base = format!("http://127.0.0.1:{port}");
    let client = reqwest::Client::new();
    let mut ready = false;
    for _ in 0..100 {
        if client
            .get(format!("{base}/health/ready"))
            .send()
            .await
            .is_ok_and(|r| r.status().is_success())
        {
            ready = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    if !ready {
        let _ = server.kill().await;
        return Ok(CheckOutcome {
            passed: false,
            detail: format!("the dev engine did not become ready on port {port}"),
        });
    }
    let api = format!("{base}/api/v1");
    let file = level2.display().to_string();
    let common = |sub: &[&str]| -> Vec<String> {
        let mut args: Vec<String> = sub.iter().map(ToString::to_string).collect();
        args.extend([
            "--url".into(),
            api.clone(),
            "--tenant-id".into(),
            "demo".into(),
        ]);
        args
    };
    let preflight = run_orch8(
        &common(&["sequence", "preflight", "--file", &file]),
        dir,
        Duration::from_secs(60),
    )
    .await;
    let create = run_orch8(
        &common(&["sequence", "create", "--file", &file]),
        dir,
        Duration::from_secs(60),
    )
    .await;
    let _ = server.kill().await;
    let (preflight_ok, preflight_out) = preflight?;
    let (create_ok, create_out) = create?;
    let passed = preflight_ok && create_ok;
    Ok(CheckOutcome {
        passed,
        detail: if passed {
            format!("preflight passed and the sequence was published to the dev engine at {api}")
        } else {
            format!(
                "preflight ok={preflight_ok}, create ok={create_ok}:\n{}\n{}",
                tail(&preflight_out, 8),
                tail(&create_out, 8)
            )
        },
    })
}

// ---------------------------------------------------------------------------
// Entry point
// ---------------------------------------------------------------------------

fn prompt(question: &str) -> Result<String> {
    eprint!("{question} ");
    std::io::stderr().flush()?;
    let mut answer = String::new();
    std::io::stdin().read_line(&mut answer)?;
    Ok(answer.trim().to_ascii_lowercase())
}

#[allow(clippy::too_many_lines)]
pub async fn run(cmd: LearnCmd) -> Result<()> {
    let steps = steps();
    let dir = cmd.dir.clone();
    if cmd.reset {
        let _ = std::fs::remove_file(Progress::path(&dir));
        let _ = std::fs::remove_dir_all(dir.join(".orch8").join("learn"));
        println!("learn progress reset");
        if !cmd.list && cmd.step.is_none() && !cmd.check {
            return Ok(());
        }
    }
    let mut progress = Progress::load(&dir)?;
    if cmd.list {
        print!("{}", render_list(&steps, &progress));
        return Ok(());
    }
    let mut index = match cmd.step {
        Some(0) => bail!("steps are numbered from 1"),
        Some(n) if n > steps.len() => bail!("there are only {} steps", steps.len()),
        Some(n) => n - 1,
        None => next_index(&steps, &progress),
    };
    let interactive = std::io::stdin().is_terminal() && std::io::stdout().is_terminal();

    if !interactive || cmd.check {
        let step = &steps[index];
        print!("{}", render_step(step, index, steps.len(), &dir));
        if cmd.check {
            let Some(check) = check_for(step) else {
                bail!("step {} ({}) has no automated check", index + 1, step.id);
            };
            let outcome = run_check(step, &check, &dir).await?;
            report_outcome(&outcome);
            if !outcome.passed {
                bail!("check failed for step {}", index + 1);
            }
            progress.complete(&step.id);
            progress.current = steps.get(index + 1).map(|s| s.id.clone());
            progress.save(&dir)?;
        } else {
            println!(
                "\n{}",
                "(no TTY: showing the step only — run `orch8 learn` in a terminal, or add --check)"
                    .dimmed()
            );
        }
        return Ok(());
    }

    loop {
        let step = &steps[index];
        println!();
        print!("{}", render_step(step, index, steps.len(), &dir));
        progress.current = Some(step.id.clone());
        progress.save(&dir)?;
        let check = check_for(step);
        let question = if check.is_some() {
            "[Enter] run the check · (s)kip · (b)ack · (l)ist · (q)uit ›"
        } else {
            "[Enter] mark done · (s)kip · (b)ack · (l)ist · (q)uit ›"
        };
        match prompt(question)?.as_str() {
            "q" | "quit" => break,
            "l" | "list" => {
                print!("{}", render_list(&steps, &progress));
                continue;
            }
            "b" | "back" => {
                index = index.saturating_sub(1);
                continue;
            }
            "s" | "skip" => {}
            _ => {
                if let Some(check) = check {
                    let outcome = match run_check(step, &check, &dir).await {
                        Ok(outcome) => outcome,
                        Err(error) => CheckOutcome {
                            passed: false,
                            detail: format!("{error:#}"),
                        },
                    };
                    report_outcome(&outcome);
                    if !outcome.passed {
                        println!("{}", "Fix the issue and press Enter to retry.".yellow());
                        continue;
                    }
                }
                progress.complete(&step.id);
                progress.save(&dir)?;
            }
        }
        if index + 1 >= steps.len() {
            println!(
                "\n{}",
                "You finished every quick-start step. 🎉".green().bold()
            );
            break;
        }
        index += 1;
    }
    progress.save(&dir)?;
    println!("progress saved to {}", Progress::path(&dir).display());
    Ok(())
}

fn report_outcome(outcome: &CheckOutcome) {
    if outcome.passed {
        println!("{} {}", "✓ check passed:".green().bold(), outcome.detail);
    } else {
        println!("{} {}", "✗ check failed:".red().bold(), outcome.detail);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_level_parses_into_numbered_steps() {
        let steps = steps();
        for level in 1..=6 {
            let count = steps.iter().filter(|s| s.level == level).count();
            assert!(count >= 5, "level {level} has only {count} steps");
        }
        let first = &steps[0];
        assert_eq!(first.id, "1.1");
        assert_eq!(first.title, "Create a workspace");
        assert!(first.level_title.starts_with("Level 1"));
        assert!(first.code.iter().any(|b| b.lang == "json"));
        assert!(first.code.iter().any(|b| b.lang == "bash"));
        // Checkpoint / troubleshooting sections are not steps.
        assert!(steps.iter().all(|s| s.title != "Checkpoint"));
        // Step ids are unique.
        let mut ids: Vec<&str> = steps.iter().map(|s| s.id.as_str()).collect();
        ids.sort_unstable();
        ids.dedup();
        assert_eq!(ids.len(), steps.len());
    }

    #[test]
    fn automated_checks_point_at_steps_that_exist() {
        let steps = steps();
        let checked: Vec<&str> = steps
            .iter()
            .filter(|s| check_for(s).is_some())
            .map(|s| s.id.as_str())
            .collect();
        assert_eq!(checked, ["1.1", "1.2", "1.4", "2.1", "2.2", "2.3", "3.4"]);
        // Titles still match what the checks assume.
        let title = |id: &str| steps.iter().find(|s| s.id == id).unwrap().title.clone();
        assert_eq!(title("1.2"), "Run exactly one instance");
        assert_eq!(title("1.4"), "See validation fail safely");
        assert_eq!(title("2.2"), "Run the paid path");
        assert_eq!(title("3.4"), "Preflight and publish the sequence");
    }

    #[tokio::test]
    async fn write_sequence_check_writes_a_valid_workspace_file() {
        let dir = tempfile::tempdir().unwrap();
        let steps = steps();
        for id in ["1.1", "2.1"] {
            let step = steps.iter().find(|s| s.id == id).unwrap();
            let outcome = run_check(step, &check_for(step).unwrap(), dir.path())
                .await
                .unwrap();
            assert!(outcome.passed, "{}", outcome.detail);
        }
        assert!(
            dir.path()
                .join(".orch8/learn/level-1/sequence.json")
                .is_file()
        );
        assert!(
            dir.path()
                .join(".orch8/learn/level-2/sequence.json")
                .is_file()
        );
    }

    #[test]
    fn progress_round_trips_and_tracks_the_next_step() {
        let dir = tempfile::tempdir().unwrap();
        let steps = steps();
        let mut progress = Progress::load(dir.path()).unwrap();
        assert_eq!(next_index(&steps, &progress), 0);
        progress.complete("1.1");
        progress.complete("1.1");
        progress.save(dir.path()).unwrap();
        let loaded = Progress::load(dir.path()).unwrap();
        assert_eq!(loaded.completed, ["1.1"]);
        assert_eq!(next_index(&steps, &loaded), 1);
        std::fs::write(dir.path().join(".orch8/learn.json"), "{oops").unwrap();
        assert!(
            Progress::load(dir.path())
                .unwrap_err()
                .to_string()
                .contains("--reset")
        );
    }

    #[test]
    fn rendering_shows_prose_code_and_check_kind() {
        let steps = steps();
        let dir = Path::new("/tmp/project");
        let text = render_step(&steps[1], 1, steps.len(), dir);
        assert!(text.contains("Run exactly one instance"), "{text}");
        assert!(
            text.contains("orch8 dev . --skip-timers --no-server --once"),
            "{text}"
        );
        assert!(text.contains("Automated check"), "{text}");
        let manual = steps.iter().position(|s| s.id == "1.3").unwrap();
        assert!(render_step(&steps[manual], manual, steps.len(), dir).contains("Manual step"));
        let list = render_list(
            &steps,
            &Progress {
                completed: vec!["1.1".into()],
                ..Progress::default()
            },
        );
        assert!(list.contains("1/"), "{list}");
        assert!(list.contains("[auto]"));
    }
}
