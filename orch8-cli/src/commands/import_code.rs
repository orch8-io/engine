//! Static extraction of durable-step patterns from Temporal, Inngest and
//! `BullMQ` TypeScript / JavaScript sources.
//!
//! These engines define workflows in code, so a faithful translation needs
//! the program's semantics. This importer does not execute or type-check the
//! source: it tokenizes it, finds the documented durable primitives, and
//! rebuilds the control structure around them:
//!
//! | Source | Orch8 |
//! |---|---|
//! | Inngest `step.run(id, fn)` / Temporal activity call / `BullMQ` job | worker step (the code becomes a worker handler) |
//! | `step.sleep` / `step.sleepUntil` / Temporal `sleep` | delayed `noop` |
//! | `step.waitForEvent` | `wait_for_event` (+ `try_catch` for the timeout-returns-null case) |
//! | Temporal `condition(fn, timeout)` | `wait_for_input` gate resolved by a signal |
//! | `step.invoke` / `executeChild` | `sub_sequence` |
//! | `step.sendEvent` | `emit_event` |
//! | `Promise.all([...])` / `Promise.race([...])` | `parallel` / `race` |
//! | `if` / `for..of` / `while` / `try..catch..finally` around steps | `router` / `for_each` / `loop` / `try_catch` |
//! | `CancellationScope.nonCancellable` | `cancellation_scope` |
//! | `BullMQ` `FlowProducer` children | `parallel` children, then the parent job |
//!
//! Conditions are translated when they only read the workflow input and
//! earlier step results; anything else is gated on an explicit
//! `data.todo_*` flag. Every construct the importer cannot translate
//! faithfully is listed in the report's `unmapped` section with `file:line`.

#![allow(clippy::too_many_lines, clippy::too_many_arguments)]

use std::collections::HashMap;
use std::path::Path;

use anyhow::{Context, Result, bail};
use serde_json::{Map, Value, json};

use super::{
    Builder, Conversion, ConvertOptions, TriggerSuggestion, UnmappedConstruct, finish, non_empty,
    slug, snake,
};

/// One source file.
#[derive(Debug, Clone)]
pub struct SourceFile {
    pub path: String,
    pub text: String,
}

const SOURCE_EXTENSIONS: [&str; 6] = ["ts", "tsx", "mts", "js", "mjs", "cjs"];
const MAX_FILES: usize = 2_000;

/// Read a file, or every TS/JS file under a directory (skipping
/// `node_modules`, build output and declaration files).
pub fn load_sources(path: &Path) -> Result<Vec<SourceFile>> {
    let mut out = Vec::new();
    if path.is_dir() {
        let mut stack = vec![path.to_path_buf()];
        while let Some(dir) = stack.pop() {
            let mut entries: Vec<_> = std::fs::read_dir(&dir)
                .with_context(|| format!("failed to read {}", dir.display()))?
                .filter_map(Result::ok)
                .map(|e| e.path())
                .collect();
            entries.sort();
            for entry in entries {
                let name = entry.file_name().and_then(|n| n.to_str()).unwrap_or("");
                if entry.is_dir() {
                    if !matches!(name, "node_modules" | "dist" | "build" | "lib" | ".git")
                        && !name.starts_with('.')
                    {
                        stack.push(entry);
                    }
                } else if SOURCE_EXTENSIONS
                    .iter()
                    .any(|ext| entry.extension().and_then(|e| e.to_str()) == Some(ext))
                    && !name.ends_with(".d.ts")
                {
                    if out.len() >= MAX_FILES {
                        bail!(
                            "more than {MAX_FILES} source files under {}",
                            path.display()
                        );
                    }
                    out.push(SourceFile {
                        path: entry.display().to_string(),
                        text: std::fs::read_to_string(&entry)
                            .with_context(|| format!("failed to read {}", entry.display()))?,
                    });
                }
            }
        }
        out.sort_by(|a, b| a.path.cmp(&b.path));
    } else {
        out.push(SourceFile {
            path: path.display().to_string(),
            text: std::fs::read_to_string(path)
                .with_context(|| format!("failed to read {}", path.display()))?,
        });
    }
    if out.is_empty() {
        bail!("no .ts/.js sources found under {}", path.display());
    }
    Ok(out)
}

// ---------------------------------------------------------------------------
// Tokenizer
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq)]
enum Tok {
    Ident(String),
    Str(String),
    /// Template literal: raw text, and whether it has `${}` substitutions.
    Tpl(String, bool),
    Num(String),
    Punct(&'static str),
}

#[derive(Debug, Clone)]
struct Token {
    tok: Tok,
    line: usize,
    start: usize,
    end: usize,
}

const PUNCTS: [&str; 52] = [
    ">>>=", "===", "!==", "...", "**=", "<<=", ">>=", ">>>", "&&=", "||=", "??=", "=>", "==", "!=",
    "<=", ">=", "&&", "||", "??", "?.", "++", "--", "+=", "-=", "*=", "/=", "%=", "&=", "|=", "^=",
    "**", "<<", ">>", "{", "}", "(", ")", "[", "]", ";", ",", ".", "<", ">", "+", "-", "*", "/",
    "%", "&", "|", "^",
];
const PUNCTS_1: [&str; 7] = ["!", "~", "?", ":", "=", "@", "#"];

fn tokenize(text: &str) -> Vec<Token> {
    let bytes = text.as_bytes();
    let mut out = Vec::new();
    let mut i = 0;
    let mut line = 1;
    while i < bytes.len() {
        let c = bytes[i];
        if c == b'\n' {
            line += 1;
            i += 1;
            continue;
        }
        if c.is_ascii_whitespace() {
            i += 1;
            continue;
        }
        if text[i..].starts_with("//") {
            while i < bytes.len() && bytes[i] != b'\n' {
                i += 1;
            }
            continue;
        }
        if text[i..].starts_with("/*") {
            i += 2;
            while i < bytes.len() && !text[i..].starts_with("*/") {
                if bytes[i] == b'\n' {
                    line += 1;
                }
                i += 1;
            }
            i = (i + 2).min(bytes.len());
            continue;
        }
        let start = i;
        let start_line = line;
        if c == b'"' || c == b'\'' {
            let mut s = String::new();
            i += 1;
            while i < bytes.len() && bytes[i] != c && bytes[i] != b'\n' {
                if bytes[i] == b'\\' && i + 1 < bytes.len() {
                    let esc = bytes[i + 1];
                    s.push(match esc {
                        b'n' => '\n',
                        b't' => '\t',
                        other => other as char,
                    });
                    i += 2;
                    continue;
                }
                let ch = text[i..].chars().next().unwrap_or(' ');
                s.push(ch);
                i += ch.len_utf8();
            }
            i = (i + 1).min(bytes.len());
            out.push(Token {
                tok: Tok::Str(s),
                line: start_line,
                start,
                end: i,
            });
            continue;
        }
        if c == b'`' {
            let mut depth = 0usize;
            let mut subst = false;
            i += 1;
            while i < bytes.len() {
                match bytes[i] {
                    b'\\' => i += 1,
                    b'\n' => line += 1,
                    b'`' if depth == 0 => break,
                    b'$' if bytes.get(i + 1) == Some(&b'{') => {
                        subst = true;
                        depth += 1;
                        i += 1;
                    }
                    b'}' if depth > 0 => depth -= 1,
                    _ => {}
                }
                i += 1;
            }
            let raw = text
                .get(start + 1..i.min(bytes.len()))
                .unwrap_or("")
                .to_string();
            i = (i + 1).min(bytes.len());
            out.push(Token {
                tok: Tok::Tpl(raw, subst),
                line: start_line,
                start,
                end: i,
            });
            continue;
        }
        if c.is_ascii_digit() || (c == b'.' && bytes.get(i + 1).is_some_and(u8::is_ascii_digit)) {
            while i < bytes.len()
                && (bytes[i].is_ascii_alphanumeric() || bytes[i] == b'.' || bytes[i] == b'_')
            {
                i += 1;
            }
            out.push(Token {
                tok: Tok::Num(text[start..i].replace('_', "")),
                line: start_line,
                start,
                end: i,
            });
            continue;
        }
        if c.is_ascii_alphabetic() || c == b'_' || c == b'$' {
            while i < bytes.len()
                && (bytes[i].is_ascii_alphanumeric() || bytes[i] == b'_' || bytes[i] == b'$')
            {
                i += 1;
            }
            out.push(Token {
                tok: Tok::Ident(text[start..i].to_string()),
                line: start_line,
                start,
                end: i,
            });
            continue;
        }
        if let Some(p) = PUNCTS
            .iter()
            .chain(PUNCTS_1.iter())
            .find(|p| text[i..].starts_with(**p))
        {
            i += p.len();
            out.push(Token {
                tok: Tok::Punct(p),
                line: start_line,
                start,
                end: i,
            });
            continue;
        }
        // Non-ASCII or unknown byte: skip the whole char.
        i += text[i..].chars().next().map_or(1, char::len_utf8);
    }
    out
}

/// Tokens of one file plus the matching-bracket table.
struct Src<'a> {
    file: &'a SourceFile,
    toks: Vec<Token>,
    pair: Vec<Option<usize>>,
}

impl<'a> Src<'a> {
    fn new(file: &'a SourceFile) -> Self {
        let toks = tokenize(&file.text);
        let mut pair = vec![None; toks.len()];
        let mut stack: Vec<(usize, &str)> = Vec::new();
        for (i, t) in toks.iter().enumerate() {
            if let Tok::Punct(p) = t.tok {
                match p {
                    "(" | "[" | "{" => stack.push((i, p)),
                    ")" | "]" | "}" => {
                        let open = match p {
                            ")" => "(",
                            "]" => "[",
                            _ => "{",
                        };
                        if let Some(pos) = stack.iter().rposition(|(_, o)| *o == open) {
                            let (j, _) = stack[pos];
                            stack.truncate(pos);
                            pair[i] = Some(j);
                            pair[j] = Some(i);
                        }
                    }
                    _ => {}
                }
            }
        }
        Self { file, toks, pair }
    }

    fn is(&self, i: usize, p: &str) -> bool {
        matches!(self.toks.get(i).map(|t| &t.tok), Some(Tok::Punct(q)) if *q == p)
    }

    fn ident(&self, i: usize) -> Option<&str> {
        match self.toks.get(i).map(|t| &t.tok) {
            Some(Tok::Ident(s)) => Some(s),
            _ => None,
        }
    }

    fn is_ident(&self, i: usize, name: &str) -> bool {
        self.ident(i) == Some(name)
    }

    fn line(&self, i: usize) -> usize {
        self.toks.get(i).map_or(0, |t| t.line)
    }

    /// Source text of tokens `[a, b)`, whitespace-collapsed.
    fn text(&self, a: usize, b: usize) -> String {
        if a >= b || b > self.toks.len() {
            return String::new();
        }
        let raw = &self.file.text[self.toks[a].start..self.toks[b - 1].end];
        let collapsed: Vec<&str> = raw.split_whitespace().collect();
        let s = collapsed.join(" ");
        if s.len() > 120 {
            let mut end = 117;
            while !s.is_char_boundary(end) {
                end -= 1;
            }
            format!("{}...", &s[..end])
        } else {
            s
        }
    }

    /// Split `[a, b)` at top-level commas.
    fn split_commas(&self, a: usize, b: usize) -> Vec<(usize, usize)> {
        let mut out = Vec::new();
        let mut start = a;
        let mut i = a;
        while i < b {
            if let Some(Tok::Punct(p)) = self.toks.get(i).map(|t| &t.tok)
                && matches!(*p, "(" | "[" | "{")
                && let Some(close) = self.pair[i]
            {
                i = close + 1;
                continue;
            }
            if self.is(i, ",") {
                out.push((start, i));
                start = i + 1;
            }
            i += 1;
        }
        if start < b {
            out.push((start, b));
        }
        out
    }

    /// Arguments of a call whose `(` is at `open`.
    fn args(&self, open: usize) -> (Vec<(usize, usize)>, usize) {
        let close = self.pair[open].unwrap_or(self.toks.len().saturating_sub(1));
        (self.split_commas(open + 1, close), close + 1)
    }

    /// `[a, b)` without a leading `await` / trailing `as T` / `!`.
    fn trim(&self, mut a: usize, mut b: usize) -> (usize, usize) {
        while a < b && (self.is_ident(a, "await") || self.is_ident(a, "void")) {
            a += 1;
        }
        if let Some(pos) = (a..b).find(|&i| self.is_ident(i, "as") || self.is_ident(i, "satisfies"))
        {
            b = pos;
        }
        while b > a && self.is(b - 1, "!") {
            b -= 1;
        }
        (a, b)
    }

    /// Parse a literal-ish value.
    fn lit(&self, a: usize, b: usize) -> Lit {
        let (a, b) = self.trim(a, b);
        if a >= b {
            return Lit::Expr(String::new(), a, b);
        }
        if b - a == 1 {
            match &self.toks[a].tok {
                Tok::Str(s) => return Lit::Str(s.clone()),
                Tok::Tpl(s, false) => return Lit::Str(s.clone()),
                Tok::Num(n) => {
                    if let Ok(v) = n.parse::<f64>() {
                        return Lit::Num(v);
                    }
                }
                Tok::Ident(s) if s == "true" || s == "false" => return Lit::Bool(s == "true"),
                Tok::Ident(s) if s == "null" || s == "undefined" => return Lit::Null,
                _ => {}
            }
        }
        if b - a == 2
            && self.is(a, "-")
            && let Tok::Num(n) = &self.toks[a + 1].tok
            && let Ok(v) = n.parse::<f64>()
        {
            return Lit::Num(-v);
        }
        if self.is(a, "{") && self.pair[a] == Some(b - 1) {
            let mut fields = Vec::new();
            for (s, e) in self.split_commas(a + 1, b - 1) {
                if s >= e {
                    continue;
                }
                if self.is(s, "...") {
                    fields.push((
                        "...".to_string(),
                        Lit::Expr(self.text(s, e), s, e),
                        self.line(s),
                    ));
                    continue;
                }
                let key = match &self.toks[s].tok {
                    Tok::Ident(k) | Tok::Str(k) | Tok::Num(k) => Some(k.clone()),
                    _ => None,
                };
                match (key, self.is(s + 1, ":")) {
                    (Some(k), true) => fields.push((k, self.lit(s + 2, e), self.line(s))),
                    (Some(k), false) if e == s + 1 => {
                        fields.push((k, Lit::Expr(self.text(s, e), s, e), self.line(s)));
                    }
                    _ => fields.push(("?".into(), Lit::Expr(self.text(s, e), s, e), self.line(s))),
                }
            }
            return Lit::Obj(fields);
        }
        if self.is(a, "[") && self.pair[a] == Some(b - 1) {
            return Lit::Arr(
                self.split_commas(a + 1, b - 1)
                    .into_iter()
                    .filter(|(s, e)| s < e)
                    .map(|(s, e)| self.lit(s, e))
                    .collect(),
            );
        }
        Lit::Expr(self.text(a, b), a, b)
    }
}

/// A parsed literal; `Expr` keeps the source text and token range.
#[derive(Debug, Clone)]
enum Lit {
    Str(String),
    Num(f64),
    Bool(bool),
    Null,
    Obj(Vec<(String, Lit, usize)>),
    Arr(Vec<Lit>),
    Expr(String, usize, usize),
}

impl Lit {
    fn get(&self, key: &str) -> Option<&Lit> {
        match self {
            Lit::Obj(fields) => fields.iter().find(|(k, _, _)| k == key).map(|(_, v, _)| v),
            _ => None,
        }
    }
    fn str(&self) -> Option<&str> {
        match self {
            Lit::Str(s) => Some(s),
            _ => None,
        }
    }
    fn num(&self) -> Option<f64> {
        match self {
            Lit::Num(n) => Some(*n),
            _ => None,
        }
    }
}

/// `"1h"`, `"30 minutes"`, `"1d2h"`, `5000` → milliseconds.
fn duration_ms(lit: &Lit) -> Option<u64> {
    match lit {
        #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
        Lit::Num(n) if *n >= 0.0 => Some(*n as u64),
        Lit::Str(s) => parse_duration(s),
        _ => None,
    }
}

fn parse_duration(text: &str) -> Option<u64> {
    let s = text.trim().to_ascii_lowercase();
    if s.is_empty() {
        return None;
    }
    if let Ok(n) = s.parse::<u64>() {
        return Some(n);
    }
    let mut total = 0f64;
    let mut rest = s.as_str();
    let mut any = false;
    while !rest.is_empty() {
        rest = rest.trim_start();
        let num_end = rest
            .find(|c: char| !(c.is_ascii_digit() || c == '.'))
            .unwrap_or(rest.len());
        if num_end == 0 {
            return None;
        }
        let n: f64 = rest[..num_end].parse().ok()?;
        rest = rest[num_end..].trim_start();
        let unit_end = rest
            .find(|c: char| !c.is_ascii_alphabetic())
            .unwrap_or(rest.len());
        let unit = &rest[..unit_end];
        rest = &rest[unit_end..];
        let factor = match unit {
            "ms" | "msec" | "msecs" | "millisecond" | "milliseconds" => 1.0,
            "" | "s" | "sec" | "secs" | "second" | "seconds" => 1_000.0,
            "m" | "min" | "mins" | "minute" | "minutes" => 60_000.0,
            "h" | "hr" | "hrs" | "hour" | "hours" => 3_600_000.0,
            "d" | "day" | "days" => 86_400_000.0,
            "w" | "week" | "weeks" => 604_800_000.0,
            _ => return None,
        };
        total += n * factor;
        any = true;
    }
    #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
    any.then_some(total.round() as u64)
}

// ---------------------------------------------------------------------------
// Statement tree
// ---------------------------------------------------------------------------

/// A classified durable call.
#[derive(Debug, Clone)]
struct Call {
    /// Dialect-specific kind, e.g. `step.run`, `activity`, `sleep`.
    kind: String,
    /// Activity / method name.
    name: String,
    args: Vec<(usize, usize)>,
    /// First token of the call (for line numbers and snippets).
    at: usize,
    /// End of the call (exclusive).
    end: usize,
    /// `const x = await <call>` binds `x`.
    bind: Option<String>,
}

#[derive(Debug, Clone)]
enum Node {
    Call(Call),
    If {
        at: usize,
        cond: (usize, usize),
        then: Vec<Node>,
        els: Vec<Node>,
    },
    ForOf {
        at: usize,
        var: String,
        iter: (usize, usize),
        body: Vec<Node>,
    },
    Loop {
        at: usize,
        cond: Option<(usize, usize)>,
        body: Vec<Node>,
    },
    Try {
        at: usize,
        body: Vec<Node>,
        catch: Option<(Vec<Node>, bool)>,
        finally: Option<Vec<Node>>,
    },
    Group {
        at: usize,
        race: bool,
        branches: Vec<Vec<Node>>,
        /// `Promise.all(items.map((x) => ...))`: (collection, var).
        dynamic: Option<((usize, usize), String)>,
    },
    Scope {
        body: Vec<Node>,
    },
    Other {
        at: usize,
        construct: String,
        body: Vec<Node>,
    },
    /// A known construct with no block equivalent (reported, no block).
    Note {
        at: usize,
        construct: String,
        reason: String,
    },
}

fn has_calls(nodes: &[Node]) -> bool {
    nodes.iter().any(|n| match n {
        Node::Call(_) => true,
        Node::If { then, els, .. } => has_calls(then) || has_calls(els),
        Node::ForOf { body, .. }
        | Node::Loop { body, .. }
        | Node::Scope { body }
        | Node::Other { body, .. } => has_calls(body),
        Node::Try {
            body,
            catch,
            finally,
            ..
        } => {
            has_calls(body)
                || catch.as_ref().is_some_and(|(c, _)| has_calls(c))
                || finally.as_ref().is_some_and(|f| has_calls(f))
        }
        Node::Group { branches, .. } => branches.iter().any(|b| has_calls(b)),
        Node::Note { .. } => false,
    })
}

/// Dialect hook: recognize a durable call starting at token `i`.
trait Classify {
    fn classify(&self, src: &Src<'_>, i: usize) -> Option<Call>;
    /// Constructs worth reporting even without a block (e.g. `setHandler`).
    fn note(&self, _src: &Src<'_>, _i: usize) -> Option<(String, String, usize)> {
        None
    }
}

struct Parser<'s, 'a, C: Classify> {
    src: &'s Src<'a>,
    dialect: &'s C,
    /// First token of the workflow body: a `return` anywhere else is an
    /// early exit from a nested construct.
    top: usize,
}

impl<C: Classify> Parser<'_, '_, C> {
    /// A `{ block }` or a single statement starting at `i`; returns the body
    /// range and the index after it.
    fn stmt(&self, i: usize, end: usize) -> ((usize, usize), usize) {
        if self.src.is(i, "{")
            && let Some(close) = self.src.pair[i]
        {
            return ((i + 1, close), close + 1);
        }
        let mut j = i;
        while j < end {
            if self.src.is(j, ";") {
                return ((i, j), j + 1);
            }
            if let Some(Tok::Punct(p)) = self.src.toks.get(j).map(|t| &t.tok)
                && matches!(*p, "(" | "[" | "{")
                && let Some(close) = self.src.pair[j]
            {
                j = close + 1;
                continue;
            }
            if self.src.is(j, "}") {
                return ((i, j), j);
            }
            j += 1;
        }
        ((i, end), end)
    }

    /// The variable a call result is bound to (`const x = await call`).
    fn binding(&self, at: usize) -> Option<String> {
        let mut k = at;
        if k > 0 && self.src.is_ident(k - 1, "await") {
            k -= 1;
        }
        if k >= 2 && self.src.is(k - 1, "=") {
            let name = self.src.ident(k - 2)?;
            if !matches!(name, "const" | "let" | "var") {
                return Some(name.to_string());
            }
        }
        None
    }

    fn seq(&self, start: usize, end: usize) -> Vec<Node> {
        let src = self.src;
        let mut out = Vec::new();
        let mut i = start;
        while i < end {
            match src.ident(i) {
                Some("return" | "throw") if start != self.top => {
                    let (_, j) = self.stmt(i, end);
                    let keyword = src.ident(i).unwrap_or("return");
                    out.push(Node::Note {
                        at: i,
                        construct: src.text(i, j.min(end)),
                        reason: if keyword == "return" {
                            "early return inside a branch/loop: Orch8 continues with the blocks after the \
                             enclosing block — move the rest of the workflow into the other branch"
                                .into()
                        } else {
                            "throw inside a branch/loop: add a `fail` step at this point".into()
                        },
                    });
                    i = j.max(i + 1);
                }
                Some("if") if src.is(i + 1, "(") => {
                    let close = src.pair[i + 1].unwrap_or(end);
                    let ((ta, tb), mut j) = self.stmt(close + 1, end);
                    let then = self.seq(ta, tb);
                    let mut els = Vec::new();
                    if src.is_ident(j, "else") {
                        let ((ea, eb), k) = self.stmt(j + 1, end);
                        // `else if (...) ...` is a single statement.
                        let (ea, eb, k) = if src.is_ident(j + 1, "if") {
                            let (_, k2) = self.else_if_end(j + 1, end);
                            (j + 1, k2, k2)
                        } else {
                            (ea, eb, k)
                        };
                        els = self.seq(ea, eb);
                        j = k;
                    }
                    out.push(Node::If {
                        at: i,
                        cond: (i + 2, close),
                        then,
                        els,
                    });
                    i = j;
                }
                Some("for")
                    if src.is(i + 1, "(")
                        || (src.is_ident(i + 1, "await") && src.is(i + 2, "(")) =>
                {
                    let open = if src.is(i + 1, "(") { i + 1 } else { i + 2 };
                    let close = src.pair[open].unwrap_or(end);
                    let ((ba, bb), j) = self.stmt(close + 1, end);
                    let body = self.seq(ba, bb);
                    let of = (open + 1..close).find(|&k| src.is_ident(k, "of"));
                    let decl = src
                        .ident(open + 1)
                        .is_some_and(|d| matches!(d, "const" | "let" | "var"));
                    match (of, decl, src.ident(open + 2)) {
                        (Some(of), true, Some(var)) if of == open + 3 => out.push(Node::ForOf {
                            at: i,
                            var: var.to_string(),
                            iter: (of + 1, close),
                            body,
                        }),
                        _ => out.push(Node::Loop {
                            at: i,
                            cond: None,
                            body,
                        }),
                    }
                    i = j;
                }
                Some("while") if src.is(i + 1, "(") => {
                    let close = src.pair[i + 1].unwrap_or(end);
                    let ((ba, bb), j) = self.stmt(close + 1, end);
                    out.push(Node::Loop {
                        at: i,
                        cond: Some((i + 2, close)),
                        body: self.seq(ba, bb),
                    });
                    i = j;
                }
                Some("do") if src.is(i + 1, "{") => {
                    let close = src.pair[i + 1].unwrap_or(end);
                    let body = self.seq(i + 2, close);
                    let mut j = close + 1;
                    let mut cond = None;
                    if src.is_ident(j, "while") && src.is(j + 1, "(") {
                        let c = src.pair[j + 1].unwrap_or(end);
                        cond = Some((j + 2, c));
                        j = c + 1;
                    }
                    out.push(Node::Loop { at: i, cond, body });
                    i = j;
                }
                Some("try") if src.is(i + 1, "{") => {
                    let close = src.pair[i + 1].unwrap_or(end);
                    let body = self.seq(i + 2, close);
                    let mut j = close + 1;
                    let mut catch = None;
                    let mut finally = None;
                    if src.is_ident(j, "catch") {
                        let mut k = j + 1;
                        if src.is(k, "(") {
                            k = src.pair[k].unwrap_or(end) + 1;
                        }
                        if src.is(k, "{") {
                            let c = src.pair[k].unwrap_or(end);
                            let rethrows = (k + 1..c).any(|t| src.is_ident(t, "throw"));
                            catch = Some((self.seq(k + 1, c), rethrows));
                            j = c + 1;
                        }
                    }
                    if src.is_ident(j, "finally") && src.is(j + 1, "{") {
                        let c = src.pair[j + 1].unwrap_or(end);
                        finally = Some(self.seq(j + 2, c));
                        j = c + 1;
                    }
                    out.push(Node::Try {
                        at: i,
                        body,
                        catch,
                        finally,
                    });
                    i = j;
                }
                Some("switch") if src.is(i + 1, "(") => {
                    let close = src.pair[i + 1].unwrap_or(end);
                    let ((ba, bb), j) = self.stmt(close + 1, end);
                    out.push(Node::Other {
                        at: i,
                        construct: format!("switch ({})", src.text(i + 2, close)),
                        body: self.seq(ba, bb),
                    });
                    i = j;
                }
                Some("Promise") if src.is(i + 1, ".") && src.is(i + 3, "(") => {
                    let method = src.ident(i + 2).unwrap_or("");
                    let open = i + 3;
                    let (args, j) = src.args(open);
                    if !matches!(method, "all" | "allSettled" | "race" | "any") || args.is_empty() {
                        i += 1;
                        continue;
                    }
                    let (a, b) = src.trim(args[0].0, args[0].1);
                    let race = matches!(method, "race" | "any");
                    if src.is(a, "[") && src.pair[a] == Some(b - 1) {
                        let branches: Vec<Vec<Node>> = src
                            .split_commas(a + 1, b - 1)
                            .into_iter()
                            .map(|(s, e)| self.seq(s, e))
                            .collect();
                        if branches.iter().any(|b| has_calls(b)) {
                            out.push(Node::Group {
                                at: i,
                                race,
                                branches,
                                dynamic: None,
                            });
                        }
                    } else {
                        // `items.map((x) => ...)`
                        let body = self.seq(a, b);
                        if has_calls(&body) {
                            let dynamic = self.map_callback(a, b);
                            out.push(Node::Group {
                                at: i,
                                race,
                                branches: vec![body],
                                dynamic,
                            });
                        }
                    }
                    if method == "allSettled" || method == "any" {
                        out.push(Node::Note {
                            at: i,
                            construct: format!("Promise.{method}"),
                            reason: "mapped like Promise.all/race: a failing branch fails the Orch8 block \
                                     instead of being collected"
                                .into(),
                        });
                    }
                    i = j;
                }
                Some("map" | "forEach" | "flatMap" | "filter" | "reduce" | "some" | "every")
                    if i > 0 && src.is(i - 1, ".") && src.is(i + 1, "(") =>
                {
                    let (_, j) = src.args(i + 1);
                    let body = self.seq(i + 2, j - 1);
                    if has_calls(&body) {
                        out.push(Node::Other {
                            at: i,
                            construct: format!(".{}(...) callback", src.ident(i).unwrap_or("")),
                            body,
                        });
                    }
                    i = j;
                }
                Some("CancellationScope") if src.is(i + 1, ".") && src.is(i + 3, "(") => {
                    let method = src.ident(i + 2).unwrap_or("");
                    let (_, j) = src.args(i + 3);
                    let body = self.seq(i + 4, j - 1);
                    if method == "nonCancellable" {
                        out.push(Node::Scope { body });
                    } else {
                        out.push(Node::Other {
                            at: i,
                            construct: format!("CancellationScope.{method}"),
                            body,
                        });
                    }
                    i = j;
                }
                _ => {
                    if let Some(mut call) = self.dialect.classify(src, i) {
                        call.bind = self.binding(i);
                        i = call.end.max(i + 1);
                        out.push(Node::Call(call));
                    } else {
                        if let Some((construct, reason, j)) = self.dialect.note(src, i) {
                            out.push(Node::Note {
                                at: i,
                                construct,
                                reason,
                            });
                            i = j.max(i + 1);
                            continue;
                        }
                        i += 1;
                    }
                }
            }
        }
        out
    }

    /// End of an `if (...) stmt [else stmt]` chain starting at `i`.
    fn else_if_end(&self, i: usize, end: usize) -> (usize, usize) {
        let close = self.src.pair[i + 1].unwrap_or(end);
        let (_, mut j) = self.stmt(close + 1, end);
        if self.src.is_ident(j, "else") {
            if self.src.is_ident(j + 1, "if") {
                let (_, k) = self.else_if_end(j + 1, end);
                j = k;
            } else {
                let (_, k) = self.stmt(j + 1, end);
                j = k;
            }
        }
        (i, j)
    }

    /// `X.map((v) => ...)` / `X.map(async v => ...)`: (collection range, v).
    fn map_callback(&self, a: usize, b: usize) -> Option<((usize, usize), String)> {
        let src = self.src;
        let dot =
            (a..b).find(|&k| src.is(k, ".") && src.is_ident(k + 1, "map") && src.is(k + 2, "("))?;
        let mut k = dot + 3;
        if src.is_ident(k, "async") {
            k += 1;
        }
        let var = if src.is(k, "(") {
            src.ident(k + 1)?
        } else {
            src.ident(k)?
        };
        Some(((a, dot), var.to_string()))
    }
}

// ---------------------------------------------------------------------------
// Emission
// ---------------------------------------------------------------------------

/// Shared emission state for one workflow.
struct Emit<'s, 'a> {
    b: Builder,
    src: &'s Src<'a>,
    /// JS variable → Orch8 path root (`event` → `data`, `user` →
    /// `outputs.fetch_user`).
    vars: HashMap<String, String>,
}

impl Emit<'_, '_> {
    fn loc(&self, at: usize) -> String {
        format!("{}:{}", self.src.file.path, self.src.line(at))
    }

    fn unmapped(&mut self, at: usize, construct: &str, reason: &str) {
        self.b.report.unmapped.push(UnmappedConstruct {
            file: self.src.file.path.clone(),
            line: self.src.line(at),
            construct: construct.into(),
            reason: reason.into(),
        });
    }

    /// Translate a JS expression to an Orch8 expression.
    fn expr(&self, a: usize, b: usize) -> Option<String> {
        let src = self.src;
        let (a, b) = src.trim(a, b);
        if a >= b {
            return None;
        }
        let mut out: Vec<String> = Vec::new();
        let mut i = a;
        while i < b {
            match &src.toks[i].tok {
                Tok::Str(s) | Tok::Tpl(s, false) => out.push(serde_json::to_string(s).ok()?),
                Tok::Num(n) => out.push(n.parse::<f64>().ok().map(|_| n.clone())?),
                Tok::Ident(id) => match id.as_str() {
                    "true" | "false" | "null" => out.push(id.clone()),
                    "undefined" => out.push("null".into()),
                    _ => {
                        let (path, j) = self.path(i, b)?;
                        out.push(path);
                        i = j;
                        continue;
                    }
                },
                Tok::Punct(p) => out.push(
                    match *p {
                        "===" | "==" => "==",
                        "!==" | "!=" => "!=",
                        "<" | ">" | "<=" | ">=" | "&&" | "||" | "!" | "(" | ")" | "+" | "-"
                        | "*" | "/" | "?" | ":" => p,
                        _ => return None,
                    }
                    .to_string(),
                ),
                Tok::Tpl(_, true) => return None,
            }
            i += 1;
        }
        Some(
            out.join(" ")
                .replace("( ", "(")
                .replace(" )", ")")
                .replace("! ", "!"),
        )
    }

    /// `root.a?.b[0].length` → Orch8 path (`len(...)` for `.length`).
    fn path(&self, i: usize, b: usize) -> Option<(String, usize)> {
        let src = self.src;
        let root = src.ident(i)?;
        if src.is(i + 1, "(") {
            return None; // a call
        }
        let mut path = self.vars.get(root)?.clone();
        let mut j = i + 1;
        let mut length = false;
        while j < b {
            if (src.is(j, ".") || src.is(j, "?.")) && src.ident(j + 1).is_some() {
                let seg = src.ident(j + 1)?;
                if src.is(j + 2, "(") {
                    return None;
                }
                if seg == "length" {
                    length = true;
                    j += 2;
                    break;
                }
                path.push('.');
                path.push_str(seg);
                j += 2;
            } else if src.is(j, "[") {
                let close = src.pair[j]?;
                match self.src.toks.get(j + 1).map(|t| &t.tok) {
                    Some(Tok::Num(n) | Tok::Str(n)) if close == j + 2 => {
                        path.push('.');
                        path.push_str(n);
                    }
                    _ => return None,
                }
                j = close + 1;
            } else {
                break;
            }
        }
        Some((if length { format!("len({path})") } else { path }, j))
    }

    /// A literal → JSON, translating plain paths to `{{path}}` templates.
    fn json(&mut self, lit: &Lit, what: &str) -> Value {
        match lit {
            Lit::Str(s) => json!(s),
            Lit::Num(n) => json!(n),
            Lit::Bool(v) => json!(v),
            Lit::Null => Value::Null,
            Lit::Arr(items) => Value::Array(items.iter().map(|l| self.json(l, what)).collect()),
            Lit::Obj(fields) => {
                let mut map = Map::new();
                for (k, v, _) in fields {
                    if k == "..." {
                        if let Lit::Expr(src, a, _) = v {
                            let at = *a;
                            let reason =
                                format!("object spread in {what} is not expanded; kept as `{src}`");
                            self.unmapped(at, src, &reason);
                        }
                        continue;
                    }
                    map.insert(k.clone(), self.json(v, what));
                }
                Value::Object(map)
            }
            Lit::Expr(src, a, b) => {
                if let Some((path, end)) = self.path(*a, *b)
                    && end == *b
                {
                    return json!(format!("{{{{{path}}}}}"));
                }
                if src.is_empty() {
                    return Value::Null;
                }
                let at = *a;
                self.unmapped(
                    at,
                    src,
                    &format!(
                        "expression in {what} is not a plain reference to the workflow input or \
                         an earlier step result; kept as literal text"
                    ),
                );
                json!(src)
            }
        }
    }

    fn todo_flag(&mut self, at: usize, construct: &str, what: &str) -> String {
        let flag = format!("todo_{}_l{}", slug(what), self.src.line(at));
        self.unmapped(
            at,
            construct,
            &format!(
                "{what} could not be translated; the block is gated on `data.{flag} == true` until \
                 it is rewritten as an Orch8 expression"
            ),
        );
        format!("data.{flag} == true")
    }

    /// Nodes → blocks, with `call` handling the dialect's durable calls.
    fn nodes(
        &mut self,
        nodes: &[Node],
        call: &mut dyn FnMut(&mut Self, &Call) -> Vec<Value>,
    ) -> Vec<Value> {
        let mut out = Vec::new();
        for node in nodes {
            match node {
                Node::Call(c) => out.extend(call(self, c)),
                Node::Note {
                    at,
                    construct,
                    reason,
                } => self.unmapped(*at, construct, reason),
                Node::If {
                    at,
                    cond,
                    then,
                    els,
                } => {
                    if !has_calls(then) && !has_calls(els) {
                        continue;
                    }
                    let construct = format!("if ({})", self.src.text(cond.0, cond.1));
                    let condition = self
                        .expr(cond.0, cond.1)
                        .unwrap_or_else(|| self.todo_flag(*at, &construct, "if condition"));
                    let id = self.b.unique_id(&format!("if_l{}", self.src.line(*at)));
                    let then_blocks = self.nodes(then, call);
                    let then_blocks = non_empty(then_blocks, &mut self.b, &format!("{id}_then"));
                    let mut router = json!({"type": "router", "id": id, "routes": [{"condition": condition, "blocks": then_blocks}]});
                    let else_blocks = self.nodes(els, call);
                    if !else_blocks.is_empty() {
                        router["default"] = Value::Array(else_blocks);
                    }
                    let loc = self.loc(*at);
                    self.b.mapped(&loc, "if", &id, "router");
                    out.push(router);
                }
                Node::ForOf {
                    at,
                    var,
                    iter,
                    body,
                } => {
                    if !has_calls(body) {
                        continue;
                    }
                    let construct = format!("for (... of {})", self.src.text(iter.0, iter.1));
                    let collection = match self.path(iter.0, iter.1) {
                        Some((p, end)) if end == iter.1 => format!("{{{{{p}}}}}"),
                        _ => {
                            let flag = format!("todo_items_l{}", self.src.line(*at));
                            self.unmapped(*at, &construct, &format!("collection is not a plain input/step-result path; iterating `data.{flag}` until rewritten"));
                            format!("{{{{data.{flag}}}}}")
                        }
                    };
                    let item = slug(var);
                    let previous = self.vars.insert(var.clone(), item.clone());
                    let body_blocks = self.nodes(body, call);
                    restore(&mut self.vars, var, previous);
                    let id = self
                        .b
                        .unique_id(&format!("for_{item}_l{}", self.src.line(*at)));
                    let body_blocks = non_empty(body_blocks, &mut self.b, &format!("{id}_body"));
                    let loc = self.loc(*at);
                    self.b.mapped(&loc, "for...of", &id, "for_each");
                    out.push(json!({"type": "for_each", "id": id, "collection": collection, "item_var": item, "max_iterations": 10000, "body": body_blocks}));
                }
                Node::Loop { at, cond, body } => {
                    if !has_calls(body) {
                        continue;
                    }
                    let construct = cond.map_or_else(
                        || format!("loop at line {}", self.src.line(*at)),
                        |(a, b)| format!("while ({})", self.src.text(a, b)),
                    );
                    let condition = cond
                        .and_then(|(a, b)| self.expr(a, b))
                        .filter(|c| !c.contains("outputs."))
                        .unwrap_or_else(|| self.todo_flag(*at, &construct, "loop condition"));
                    let id = self.b.unique_id(&format!("loop_l{}", self.src.line(*at)));
                    let body_blocks = self.nodes(body, call);
                    let body_blocks = non_empty(body_blocks, &mut self.b, &format!("{id}_body"));
                    self.unmapped(*at, &construct, &format!("loop → `loop` `{id}`: Orch8 loop conditions read context.data only and are checked before each iteration; loop-local variables do not exist — verify termination (max_iterations 1000)"));
                    out.push(json!({"type": "loop", "id": id, "condition": condition, "max_iterations": 1000, "body": body_blocks}));
                }
                Node::Try {
                    at,
                    body,
                    catch,
                    finally,
                } => {
                    let body_blocks = self.nodes(body, call);
                    let (mut catch_nodes, rethrows) = catch.clone().unwrap_or_default();
                    // A rethrow becomes the trailing `fail` step below.
                    catch_nodes.retain(|n| !matches!(n, Node::Note { construct, .. } if construct.starts_with("throw")));
                    let mut catch_blocks = self.nodes(&catch_nodes, call);
                    let finally_blocks = finally.as_ref().map(|f| self.nodes(f, call));
                    if body_blocks.is_empty() {
                        // No durable call inside the try: only plain code can
                        // throw, so the catch path is a worker-side concern.
                        if !catch_blocks.is_empty() {
                            let n = catch_blocks.len();
                            self.unmapped(*at, "try { <no durable calls> } catch", &format!("{n} block(s) in the catch handler only run when non-durable code throws; they were not emitted — move that code into a worker"));
                        }
                        out.extend(finally_blocks.unwrap_or_default());
                        continue;
                    }
                    let id = self.b.unique_id(&format!("try_l{}", self.src.line(*at)));
                    if rethrows || catch.is_none() {
                        let fail_id = self.b.unique_id(&format!("{id}_rethrow"));
                        catch_blocks.push(json!({"type": "step", "id": fail_id, "handler": "fail", "params": {"message": format!("rethrown from {}", self.loc(*at)), "retryable": false}}));
                    }
                    let catch_blocks = non_empty(catch_blocks, &mut self.b, &format!("{id}_catch"));
                    let mut block = json!({"type": "try_catch", "id": id, "try_block": body_blocks, "catch_block": catch_blocks});
                    if let Some(f) = finally_blocks.filter(|f| !f.is_empty()) {
                        block["finally_block"] = Value::Array(f);
                    }
                    let loc = self.loc(*at);
                    self.b.mapped(&loc, "try/catch", &id, "try_catch");
                    out.push(block);
                }
                Node::Group {
                    at,
                    race,
                    branches,
                    dynamic,
                } => {
                    if let Some(((ca, cb), var)) = dynamic {
                        let construct = format!(
                            "Promise.{}({}.map(...))",
                            if *race { "race" } else { "all" },
                            self.src.text(*ca, *cb)
                        );
                        let collection = match self.path(*ca, *cb) {
                            Some((p, end)) if end == *cb => format!("{{{{{p}}}}}"),
                            _ => format!("{{{{data.todo_items_l{}}}}}", self.src.line(*at)),
                        };
                        let item = slug(var);
                        let previous = self.vars.insert(var.clone(), item.clone());
                        let body = self.nodes(&branches[0], call);
                        restore(&mut self.vars, var, previous);
                        let id = self
                            .b
                            .unique_id(&format!("for_{item}_l{}", self.src.line(*at)));
                        let body = non_empty(body, &mut self.b, &format!("{id}_body"));
                        self.unmapped(*at, &construct, &format!("dynamic fan-out → `for_each` `{id}` over `{collection}`: items run one at a time in Orch8 (the source ran them concurrently)"));
                        out.push(json!({"type": "for_each", "id": id, "collection": collection, "item_var": item, "max_iterations": 10000, "body": body}));
                        continue;
                    }
                    let mut built: Vec<Vec<Value>> = branches
                        .iter()
                        .map(|b| self.nodes(b, call))
                        .filter(|b| !b.is_empty())
                        .collect();
                    if built.len() == 1 {
                        out.append(&mut built[0]);
                        continue;
                    }
                    let kind = if *race { "race" } else { "parallel" };
                    let id = self.b.unique_id(&format!("{kind}_l{}", self.src.line(*at)));
                    let loc = self.loc(*at);
                    self.b.mapped(
                        &loc,
                        &format!("Promise.{}", if *race { "race" } else { "all" }),
                        &id,
                        kind,
                    );
                    out.push(json!({"type": kind, "id": id, "branches": built}));
                }
                Node::Scope { body } => {
                    let blocks = self.nodes(body, call);
                    if blocks.is_empty() {
                        continue;
                    }
                    let id = self.b.unique_id("non_cancellable");
                    out.push(json!({"type": "cancellation_scope", "id": id, "blocks": blocks}));
                }
                Node::Other {
                    at,
                    construct,
                    body,
                } => {
                    let blocks = self.nodes(body, call);
                    if blocks.is_empty() {
                        continue;
                    }
                    self.unmapped(*at, construct, &format!("{} block(s) inside were emitted in source order, unconditionally — restructure them by hand", blocks.len()));
                    out.extend(blocks);
                }
            }
        }
        out
    }
}

fn restore(vars: &mut HashMap<String, String>, key: &str, previous: Option<String>) {
    match previous {
        Some(p) => {
            vars.insert(key.to_string(), p);
        }
        None => {
            vars.remove(key);
        }
    }
}

/// `step(... )` shape helpers.
fn delay_step(e: &mut Emit<'_, '_>, name: &str, kind: &str, ms: u64) -> Value {
    let id = e.b.unique_id(name);
    e.b.mapped(name, kind, &id, "noop + delay");
    json!({"type": "step", "id": id, "handler": "noop", "params": {}, "delay": {"duration": ms}})
}

fn delay_until_step(e: &mut Emit<'_, '_>, name: &str, kind: &str, when: &Lit) -> Option<Value> {
    let ts = when.str()?;
    let parsed = chrono::DateTime::parse_from_rfc3339(ts).ok()?;
    let local = parsed
        .with_timezone(&chrono::Utc)
        .format("%Y-%m-%dT%H:%M:%S")
        .to_string();
    let id = e.b.unique_id(name);
    e.b.mapped(name, kind, &id, "noop + fire_at_local");
    Some(
        json!({"type": "step", "id": id, "handler": "noop", "params": {}, "delay": {"duration": 0, "fire_at_local": local, "timezone": "UTC"}}),
    )
}

fn todo_step(e: &mut Emit<'_, '_>, c: &Call, kind: &str, reason: &str) -> Value {
    let snippet = e.src.text(c.at, c.end);
    let loc = e.loc(c.at);
    let stub = e.b.todo_stub(
        &format!("{} l{}", c.name, e.src.line(c.at)),
        kind,
        &json!({ "source": loc, "code": snippet }),
        reason,
    );
    e.unmapped(c.at, &snippet, reason);
    stub
}

fn retry_json(max_attempts: u64, initial_ms: u64, multiplier: f64, max_ms: u64) -> Value {
    json!({
        "max_attempts": max_attempts.clamp(1, u64::from(u32::MAX)),
        "initial_backoff": initial_ms,
        "max_backoff": max_ms.max(initial_ms),
        "backoff_multiplier": multiplier,
    })
}

fn sequence_name(options: &ConvertOptions, fallback: &str) -> String {
    options
        .name
        .clone()
        .unwrap_or_else(|| snake(fallback).replace('_', "-"))
}

fn select<'x, T>(
    items: &'x [T],
    options: &ConvertOptions,
    name: impl Fn(&T) -> &str,
    what: &str,
) -> Result<&'x T> {
    let Some(wanted) = options.workflow.as_deref() else {
        return items
            .first()
            .with_context(|| format!("no {what} found in the source"));
    };
    items.iter().find(|i| name(i) == wanted).with_context(|| {
        format!(
            "no {what} named `{wanted}`; found: {}",
            items.iter().map(&name).collect::<Vec<_>>().join(", ")
        )
    })
}

fn other_candidates<T>(
    e: &mut Emit<'_, '_>,
    items: &[T],
    chosen: &str,
    name: impl Fn(&T) -> &str,
    what: &str,
) {
    let others: Vec<&str> = items.iter().map(&name).filter(|n| *n != chosen).collect();
    if !others.is_empty() {
        e.b.report.warnings.push(format!(
            "the source defines other {what}s ({}); convert each with `--workflow <name>`",
            others.join(", ")
        ));
    }
}

// ---------------------------------------------------------------------------
// Inngest
// ---------------------------------------------------------------------------

struct InngestDialect {
    step: String,
}

impl Classify for InngestDialect {
    fn classify(&self, src: &Src<'_>, i: usize) -> Option<Call> {
        if !src.is_ident(i, &self.step) || !src.is(i + 1, ".") || (i > 0 && src.is(i - 1, ".")) {
            return None;
        }
        let mut name = src.ident(i + 2)?.to_string();
        let mut open = i + 3;
        if name == "ai" && src.is(i + 3, ".") {
            name = format!("ai.{}", src.ident(i + 4)?);
            open = i + 5;
        }
        if !src.is(open, "(") {
            return None;
        }
        let (args, end) = src.args(open);
        Some(Call {
            kind: format!("step.{name}"),
            name,
            args,
            at: i,
            end,
            bind: None,
        })
    }
}

struct InngestFn<'s> {
    id: String,
    config: Lit,
    triggers: Vec<Lit>,
    body: (usize, usize),
    params: Vec<String>,
    at: usize,
    src: &'s Src<'s>,
}

/// Convert an Inngest function (`inngest.createFunction(...)`).
pub fn convert_inngest(files: &[SourceFile], options: &ConvertOptions) -> Result<Conversion> {
    let srcs: Vec<Src<'_>> = files.iter().map(Src::new).collect();
    let mut functions: Vec<InngestFn<'_>> = Vec::new();
    for src in &srcs {
        for i in 0..src.toks.len() {
            if !(src.is_ident(i, "createFunction")
                && src.is(i + 1, "(")
                && i > 0
                && src.is(i - 1, "."))
            {
                continue;
            }
            let (args, _) = src.args(i + 1);
            if args.len() < 2 {
                continue;
            }
            let config = src.lit(args[0].0, args[0].1);
            let mut triggers = Vec::new();
            if args.len() >= 3 {
                match src.lit(args[1].0, args[1].1) {
                    Lit::Arr(items) => triggers.extend(items),
                    other => triggers.push(other),
                }
            }
            for key in ["triggers", "trigger"] {
                match config.get(key) {
                    Some(Lit::Arr(items)) => triggers.extend(items.iter().cloned()),
                    Some(other) => triggers.push(other.clone()),
                    None => {}
                }
            }
            let (ha, hb) = *args.last().unwrap_or(&(0, 0));
            let Some(arrow) = (ha..hb).find(|&k| src.is(k, "=>")) else {
                continue;
            };
            let params = (ha..arrow)
                .filter_map(|k| src.ident(k).map(str::to_string))
                .filter(|p| p != "async")
                .collect();
            let body = if src.is(arrow + 1, "{") {
                (arrow + 2, src.pair[arrow + 1].unwrap_or(hb))
            } else {
                (arrow + 1, hb)
            };
            let id = config
                .get("id")
                .or_else(|| config.get("name"))
                .and_then(Lit::str)
                .unwrap_or("inngest-function")
                .to_string();
            functions.push(InngestFn {
                id,
                config,
                triggers,
                body,
                params,
                at: i,
                src,
            });
        }
    }
    let f = select(
        &functions,
        options,
        |f| f.id.as_str(),
        "Inngest function (createFunction)",
    )?;
    let seq_name = sequence_name(options, &f.id);
    let mut e = Emit {
        b: Builder::new("inngest", &f.id, &seq_name),
        src: f.src,
        vars: HashMap::new(),
    };
    other_candidates(
        &mut e,
        &functions,
        &f.id,
        |f| f.id.as_str(),
        "Inngest function",
    );
    // The triggering event is the instance input: `event.data.x` → `data.data.x`.
    let step_var = "step".to_string();
    if f.params.iter().any(|p| p == "event") {
        e.vars.insert("event".into(), "data".into());
    }
    if f.params.iter().any(|p| p == "events") {
        e.unmapped(
            f.at,
            "events (batchEvents)",
            "batched events are not supported: each Orch8 instance handles one event",
        );
    }

    // Function-level options.
    let retries = f.config.get("retries").and_then(Lit::num).map_or(4, |n| {
        #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
        let r = n.max(0.0) as u64;
        r
    });
    if let Lit::Obj(fields) = &f.config {
        for (key, value, line) in fields {
            let reason = match key.as_str() {
                "id" | "name" | "retries" | "triggers" | "trigger" => continue,
                "concurrency" => {
                    "per-key concurrency limits are not translated — use a queue with bounded workers or `max_instances_per_tenant`"
                }
                "throttle" | "rateLimit" => {
                    "function-level throttling is not translated — set `rate_limit_key` on the steps"
                }
                "debounce" => {
                    "debounce is not supported — dedupe at the trigger or with an idempotency key"
                }
                "idempotency" => {
                    "pass the idempotency expression as the instance `idempotency_key` when starting it"
                }
                "cancelOn" => {
                    "cancel-on-event is not translated — send an Orch8 `cancel` signal when the event arrives"
                }
                "batchEvents" => "event batching is not supported",
                "priority" => "set the instance `priority` when starting it",
                "timeouts" => "function timeouts are not translated — use step `deadline`s",
                "onFailure" => {
                    "failure handler is not translated — wrap the steps in `try_catch` or subscribe to the failure webhook"
                }
                _ => "function option not translated",
            };
            let _ = value;
            e.b.report.unmapped.push(UnmappedConstruct {
                file: f.src.file.path.clone(),
                line: *line,
                construct: format!("createFunction({{ {key} }})"),
                reason: reason.into(),
            });
        }
    }
    e.b.report.warnings.push(format!(
        "step.run steps retry up to {retries} time(s) like the Inngest function; Orch8 backoff is exponential \
         (1s, x2, max 5m), not Inngest's schedule"
    ));

    // Triggers.
    for trigger in &f.triggers {
        if let Some(event) = trigger.get("event").and_then(Lit::str) {
            let slug_value = slug(event).replace('_', "-");
            e.b.report.triggers.push(TriggerSuggestion {
                node: event.to_string(),
                kind: "event".into(),
                body: json!({
                    "slug": slug_value,
                    "sequence_name": seq_name,
                    "tenant_id": options.tenant_id,
                    "namespace": options.namespace,
                    "trigger_type": "event",
                    "config": {},
                }),
                next_step: format!(
                    "POST the body to /triggers, then fire it with the whole Inngest event \
                     (`{{\"name\": \"{event}\", \"data\": {{...}}}}`) as the body of \
                     POST /triggers/{slug_value}/fire — it becomes context.data, so `event.data.x` \
                     is `data.data.x`"
                ),
            });
            if trigger.get("if").is_some() || trigger.get("expression").is_some() {
                e.unmapped(f.at, &format!("trigger {event} if"), "trigger filter expression is not translated — filter before firing the trigger");
            }
        } else if let Some(cron) = trigger.get("cron").and_then(Lit::str) {
            let (tz, expr) = match cron.strip_prefix("TZ=").and_then(|r| r.split_once(' ')) {
                Some((tz, expr)) => (tz, expr),
                None => ("UTC", cron),
            };
            e.b.report.triggers.push(TriggerSuggestion {
                node: cron.to_string(),
                kind: "cron".into(),
                body: json!({
                    "tenant_id": options.tenant_id,
                    "namespace": options.namespace,
                    "sequence_id": "<id printed by `orch8 sequence create`>",
                    "cron_expr": expr,
                    "timezone": tz,
                }),
                next_step: "POST the body to /cron after creating the sequence (set sequence_id)"
                    .into(),
            });
        } else {
            e.unmapped(
                f.at,
                "trigger",
                "trigger is not a literal { event } / { cron } object",
            );
        }
    }

    let dialect = InngestDialect { step: step_var };
    let parser = Parser {
        src: f.src,
        dialect: &dialect,
        top: f.body.0,
    };
    let nodes = parser.seq(f.body.0, f.body.1);
    let retry = retry_json(retries + 1, 1_000, 2.0, 300_000);
    let mut handler = |e: &mut Emit<'_, '_>, c: &Call| -> Vec<Value> { inngest_call(e, c, &retry) };
    let blocks = e.nodes(&nodes, &mut handler);
    finish(e.b, options, blocks)
}

fn inngest_call(e: &mut Emit<'_, '_>, c: &Call, retry: &Value) -> Vec<Value> {
    let src = e.src;
    let id_lit = c.args.first().map(|(a, b)| src.lit(*a, *b));
    let step_id = id_lit
        .as_ref()
        .and_then(|l| match l {
            Lit::Str(s) => Some(s.clone()),
            Lit::Obj(_) => l.get("id").and_then(Lit::str).map(str::to_string),
            _ => None,
        })
        .unwrap_or_else(|| format!("{} l{}", c.name, src.line(c.at)));
    let arg = |k: usize| c.args.get(k).map(|(a, b)| src.lit(*a, *b));
    let bind = |e: &mut Emit<'_, '_>, id: &str| {
        if let Some(var) = &c.bind {
            e.vars.insert(var.clone(), format!("outputs.{id}"));
        }
    };
    match c.name.as_str() {
        "run" => {
            let handler = snake(&step_id);
            let loc = e.loc(c.at);
            let mut block = e.b.worker_stub(
                &step_id,
                "step.run",
                &handler,
                json!({ "source": loc }),
                &format!(
                    "port the step.run callback at {loc} into the Orch8 worker handler `{handler}`"
                ),
            );
            block["retry"] = retry.clone();
            let id = block["id"].as_str().unwrap_or_default().to_string();
            bind(e, &id);
            vec![block]
        }
        "sleep" => match arg(1).as_ref().and_then(duration_ms) {
            Some(ms) => vec![delay_step(e, &step_id, "step.sleep", ms)],
            None => vec![todo_step(
                e,
                c,
                "step.sleep",
                "dynamic sleep duration — Orch8 delays are static",
            )],
        },
        "sleepUntil" => {
            match arg(1).and_then(|l| delay_until_step(e, &step_id, "step.sleepUntil", &l)) {
                Some(block) => vec![block],
                None => vec![todo_step(
                    e,
                    c,
                    "step.sleepUntil",
                    "dynamic sleepUntil date — Orch8 `fire_at_local` is static",
                )],
            }
        }
        "waitForEvent" => {
            let opts = arg(1).unwrap_or(Lit::Null);
            let Some(event) = opts.get("event").and_then(Lit::str).map(str::to_string) else {
                return vec![todo_step(
                    e,
                    c,
                    "step.waitForEvent",
                    "event name is not a literal",
                )];
            };
            let id = e.b.unique_id(&step_id);
            let correlation = match opts.get("match").and_then(Lit::str) {
                // `match: "data.userId"`: the incoming event's field equals the
                // triggering event's field.
                Some(path) => json!(format!("{{{{data.{path}}}}}")),
                None => {
                    let reason = if opts.get("if").is_some() {
                        "`if` match expressions are not translated; correlating on the instance id — ingest the event with correlation_key = the instance id, or rewrite as `match`"
                    } else {
                        "no `match`: correlating on the instance id — ingest the event with that correlation key"
                    };
                    e.unmapped(c.at, &format!("step.waitForEvent({step_id})"), reason);
                    json!("{{instance_id}}")
                }
            };
            let mut gate = json!({"prompt": format!("waiting for event {event}")});
            let timeout = opts.get("timeout").and_then(duration_ms);
            if let Some(ms) = timeout {
                gate["timeout"] = json!(ms);
            }
            e.b.mapped(&step_id, "step.waitForEvent", &id, "wait_for_event");
            e.b.report.warnings.push(format!(
                "step.waitForEvent `{step_id}`: ingest `{event}` events via POST /events (or Engine::ingest_event) \
                 with the correlation key the step expects; the payload is at outputs.{id}.events"
            ));
            if let Some(var) = &c.bind
                && event
                    .chars()
                    .all(|ch| ch.is_ascii_alphanumeric() || ch == '_')
            {
                e.vars
                    .insert(var.clone(), format!("outputs.{id}.events.{event}.payload"));
            }
            let wait = json!({"type": "step", "id": id, "handler": "wait_for_event", "params": {"events": [event], "correlation_key": correlation, "join": "any"}, "wait_for_input": gate});
            if timeout.is_some() {
                // Inngest resolves `null` on timeout; Orch8 fails the gate. The
                // try_catch keeps the run going with no output, like Inngest.
                let tc = e.b.unique_id(&format!("{step_id}_or_timeout"));
                let noop = e.b.unique_id(&format!("{step_id}_timed_out"));
                return vec![
                    json!({"type": "try_catch", "id": tc, "try_block": [wait], "catch_block": [{"type": "step", "id": noop, "handler": "noop", "params": {}}]}),
                ];
            }
            vec![wait]
        }
        "sendEvent" => {
            let payload = arg(1).unwrap_or(Lit::Null);
            let events: Vec<Lit> = match payload {
                Lit::Arr(items) => items,
                other => vec![other],
            };
            let mut out = Vec::new();
            for ev in events {
                let Some(name) = ev.get("name").and_then(Lit::str).map(str::to_string) else {
                    out.push(todo_step(
                        e,
                        c,
                        "step.sendEvent",
                        "event name is not a literal",
                    ));
                    continue;
                };
                let data = ev
                    .get("data")
                    .cloned()
                    .map_or_else(|| json!({}), |d| e.json(&d, "step.sendEvent data"));
                let id = e.b.unique_id(&step_id);
                let slug_value = slug(&name).replace('_', "-");
                e.b.mapped(
                    &step_id,
                    "step.sendEvent",
                    &id,
                    &format!("emit_event {slug_value}"),
                );
                out.push(json!({"type": "step", "id": id, "handler": "emit_event", "params": {"trigger_slug": slug_value, "data": {"name": name, "data": data}}}));
            }
            out
        }
        "invoke" => {
            let opts = arg(1).unwrap_or(Lit::Null);
            let function = match opts.get("function") {
                Some(Lit::Expr(src_text, _, _)) => {
                    src_text.rsplit('.').next().unwrap_or(src_text).to_string()
                }
                Some(Lit::Str(s)) => s.clone(),
                _ => {
                    return vec![todo_step(
                        e,
                        c,
                        "step.invoke",
                        "invoked function is not a literal reference",
                    )];
                }
            };
            let data = opts
                .get("data")
                .cloned()
                .map_or_else(|| json!({}), |d| e.json(&d, "step.invoke data"));
            let id = e.b.unique_id(&step_id);
            let child = snake(&function).replace('_', "-");
            e.b.mapped(
                &step_id,
                "step.invoke",
                &id,
                &format!("sub_sequence {child}"),
            );
            e.b.report.warnings.push(format!(
                "step.invoke `{step_id}`: import `{function}` as sequence `{child}` too"
            ));
            bind(e, &id);
            vec![
                json!({"type": "sub_sequence", "id": id, "sequence_name": child, "input": {"data": data}}),
            ]
        }
        "fetch" => {
            let url = arg(0).map_or(Value::Null, |l| e.json(&l, "step.fetch url"));
            let method = arg(1)
                .and_then(|l| l.get("method").and_then(Lit::str).map(str::to_string))
                .unwrap_or_else(|| "GET".into());
            let id = e.b.unique_id(&format!("fetch_l{}", src.line(c.at)));
            e.b.mapped("step.fetch", "step.fetch", &id, "http_request");
            bind(e, &id);
            vec![
                json!({"type": "step", "id": id, "handler": "http_request", "params": {"url": url, "method": method}}),
            ]
        }
        "ai.infer" | "ai.wrap" => {
            let handler = snake(&step_id);
            let loc = e.loc(c.at);
            let block = e.b.worker_stub(
                &step_id,
                &format!("step.{}", c.name),
                &handler,
                json!({"source": loc}),
                "AI step: port to a worker, or use the built-in `llm_call` handler",
            );
            let id = block["id"].as_str().unwrap_or_default().to_string();
            bind(e, &id);
            vec![block]
        }
        other => vec![todo_step(
            e,
            c,
            &format!("step.{other}"),
            &format!("step.{other} has no Orch8 mapping yet"),
        )],
    }
}

// ---------------------------------------------------------------------------
// Temporal
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Default)]
struct ActivityOptions {
    timeout_ms: Option<u64>,
    deadline_ms: Option<u64>,
    retry: Option<Value>,
    queue: Option<String>,
    line: usize,
}

struct TemporalDialect {
    activities: HashMap<String, ActivityOptions>,
    namespaces: HashMap<String, ActivityOptions>,
}

impl Classify for TemporalDialect {
    fn classify(&self, src: &Src<'_>, i: usize) -> Option<Call> {
        let name = src.ident(i)?;
        if i > 0 && (src.is(i - 1, ".") || src.is_ident(i - 1, "function")) {
            return None;
        }
        // `acts.chargeCard(...)`
        if self.namespaces.contains_key(name) && src.is(i + 1, ".") && src.is(i + 3, "(") {
            let method = src.ident(i + 2)?;
            let (args, end) = src.args(i + 3);
            return Some(Call {
                kind: format!("activity:{name}"),
                name: method.to_string(),
                args,
                at: i,
                end,
                bind: None,
            });
        }
        let generic_call = src.is(i + 1, "<") && src.is(i + 3, ">") && src.is(i + 4, "(");
        if !(src.is(i + 1, "(") || generic_call) {
            return None;
        }
        let open = if src.is(i + 1, "(") { i + 1 } else { i + 4 };
        let kind = if self.activities.contains_key(name) {
            "activity"
        } else {
            match name {
                "sleep"
                | "condition"
                | "executeChild"
                | "startChild"
                | "continueAsNew"
                | "makeContinueAsNewFunc" => name,
                _ => return None,
            }
        };
        let (args, end) = src.args(open);
        Some(Call {
            kind: kind.to_string(),
            name: name.to_string(),
            args,
            at: i,
            end,
            bind: None,
        })
    }

    fn note(&self, src: &Src<'_>, i: usize) -> Option<(String, String, usize)> {
        let name = src.ident(i)?;
        if !src.is(i + 1, "(") || (i > 0 && src.is(i - 1, ".")) {
            return None;
        }
        let reason = match name {
            "setHandler" => {
                "signal/query/update handler: its effect on workflow state is not translated — send an Orch8 signal (e.g. `human_input:<gate>` to release a `condition` gate) or update context"
            }
            "patched" | "deprecatePatch" => {
                "versioning patch: use Orch8 sequence versions and releases instead"
            }
            "uuid4" => {
                "deterministic uuid: use `{{instance_id}}` or generate the id inside a worker"
            }
            "workflowInfo" => "workflow info is not available; use `instance_id` / context",
            _ => return None,
        };
        let (_, end) = src.args(i + 1);
        Some((src.text(i, end), reason.to_string(), end))
    }
}

fn parse_activity_options(src: &Src<'_>, lit: &Lit, e_line: usize) -> ActivityOptions {
    let _ = src;
    let mut o = ActivityOptions {
        line: e_line,
        ..ActivityOptions::default()
    };
    o.timeout_ms = lit.get("startToCloseTimeout").and_then(duration_ms);
    o.deadline_ms = lit.get("scheduleToCloseTimeout").and_then(duration_ms);
    o.queue = lit.get("taskQueue").and_then(Lit::str).map(str::to_string);
    let retry = lit.get("retry");
    let max_attempts = retry
        .and_then(|r| r.get("maximumAttempts"))
        .and_then(Lit::num)
        .filter(|n| *n > 0.0);
    let initial = retry
        .and_then(|r| r.get("initialInterval"))
        .and_then(duration_ms)
        .unwrap_or(1_000);
    let coeff = retry
        .and_then(|r| r.get("backoffCoefficient"))
        .and_then(Lit::num)
        .unwrap_or(2.0);
    let max = retry
        .and_then(|r| r.get("maximumInterval"))
        .and_then(duration_ms)
        .unwrap_or(initial.saturating_mul(100));
    #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
    let attempts = max_attempts.map_or(10, |n| n as u64);
    let mut r = retry_json(attempts, initial, coeff, max);
    if let Some(Lit::Arr(codes)) = retry.and_then(|r| r.get("nonRetryableErrorTypes")) {
        r["non_retryable_codes"] = json!(codes.iter().filter_map(Lit::str).collect::<Vec<_>>());
    }
    o.retry = Some(r);
    o
}

struct TemporalWorkflow<'s> {
    name: String,
    params: (usize, usize),
    body: (usize, usize),
    at: usize,
    src: &'s Src<'s>,
}

/// Convert a Temporal TypeScript workflow.
pub fn convert_temporal(files: &[SourceFile], options: &ConvertOptions) -> Result<Conversion> {
    let srcs: Vec<Src<'_>> = files.iter().map(Src::new).collect();
    let mut workflows: Vec<TemporalWorkflow<'_>> = Vec::new();
    let mut dialects: Vec<TemporalDialect> = Vec::new();
    let mut proxy_notes: Vec<(usize, String, usize)> = Vec::new(); // (src idx, text, line)
    let mut signals: Vec<(usize, String, usize)> = Vec::new();
    for (si, src) in srcs.iter().enumerate() {
        let mut dialect = TemporalDialect {
            activities: HashMap::new(),
            namespaces: HashMap::new(),
        };
        for i in 0..src.toks.len() {
            // const { a, b } = proxyActivities<...>({ ... })
            if matches!(
                src.ident(i),
                Some("proxyActivities" | "proxyLocalActivities")
            ) && !(i > 0 && src.is(i - 1, "."))
            {
                let mut open = i + 1;
                if src.is(open, "<") {
                    open = (open..src.toks.len())
                        .find(|&k| src.is(k, "("))
                        .unwrap_or(open);
                }
                if !src.is(open, "(") {
                    continue;
                }
                let (args, _) = src.args(open);
                let lit = args.first().map_or(Lit::Null, |(a, b)| src.lit(*a, *b));
                let opts = parse_activity_options(src, &lit, src.line(i));
                let explicit_retry = lit
                    .get("retry")
                    .and_then(|r| r.get("maximumAttempts"))
                    .and_then(Lit::num)
                    .is_some_and(|n| n > 0.0);
                if !explicit_retry {
                    proxy_notes.push((si, src.text(i, open), src.line(i)));
                }
                if lit.get("heartbeatTimeout").is_some() {
                    signals.push((si, "heartbeatTimeout".into(), src.line(i)));
                }
                // Walk back over `= const {..}` / `= const name`.
                if i >= 2 && src.is(i - 1, "=") {
                    if src.is(i - 2, "}")
                        && let Some(open_brace) = src.pair[i - 2]
                    {
                        for (a, b) in src.split_commas(open_brace + 1, i - 2) {
                            // `a`, `a: alias`
                            let name = if b > a + 2 && src.is(a + 1, ":") {
                                src.ident(a + 2)
                            } else {
                                src.ident(a)
                            };
                            if let Some(n) = name {
                                dialect.activities.insert(n.to_string(), opts.clone());
                            }
                        }
                    } else if let Some(n) = src.ident(i - 2) {
                        dialect.namespaces.insert(n.to_string(), opts.clone());
                    }
                }
            }
            // defineSignal('x') / defineSignal<[T]>('x')
            let define_open = if src.is(i + 1, "<") {
                (i + 2..src.toks.len().min(i + 32)).find(|&k| src.is(k, "("))
            } else {
                src.is(i + 1, "(").then_some(i + 1)
            };
            if matches!(
                src.ident(i),
                Some("defineSignal" | "defineQuery" | "defineUpdate")
            ) && let Some(open) = define_open
            {
                let (args, end) = src.args(open);
                let name = args.first().map(|(a, b)| src.lit(*a, *b));
                signals.push((
                    si,
                    format!(
                        "{}({})",
                        src.ident(i).unwrap_or(""),
                        name.as_ref().and_then(Lit::str).unwrap_or("?")
                    ),
                    src.line(i),
                ));
                let _ = end;
            }
            // export async function name(params) { body }
            if src.is_ident(i, "function")
                && (i > 0 && (src.is_ident(i - 1, "async") || src.is_ident(i - 1, "export")))
            {
                let exported = (i.saturating_sub(3)..i).any(|k| src.is_ident(k, "export"));
                let Some(name) = src.ident(i + 1) else {
                    continue;
                };
                let mut open = i + 2;
                if src.is(open, "<") {
                    open = (open..src.toks.len())
                        .find(|&k| src.is(k, "("))
                        .unwrap_or(open);
                }
                if !exported || !src.is(open, "(") {
                    continue;
                }
                let close = src.pair[open].unwrap_or(open);
                let Some(body_open) = (close + 1..src.toks.len()).find(|&k| src.is(k, "{")) else {
                    continue;
                };
                let body_close = src.pair[body_open].unwrap_or(body_open);
                workflows.push(TemporalWorkflow {
                    name: name.to_string(),
                    params: (open + 1, close),
                    body: (body_open + 1, body_close),
                    at: i,
                    src,
                });
            }
            // export const name = async (params) => { body }
            if src.is_ident(i, "export")
                && src.is_ident(i + 1, "const")
                && src.is(i + 3, "=")
                && src.is_ident(i + 4, "async")
                && src.is(i + 5, "(")
            {
                let Some(name) = src.ident(i + 2) else {
                    continue;
                };
                let close = src.pair[i + 5].unwrap_or(i + 5);
                let Some(arrow) = (close + 1..src.toks.len()).find(|&k| src.is(k, "=>")) else {
                    continue;
                };
                if !src.is(arrow + 1, "{") {
                    continue;
                }
                let body_close = src.pair[arrow + 1].unwrap_or(arrow + 1);
                workflows.push(TemporalWorkflow {
                    name: name.to_string(),
                    params: (i + 6, close),
                    body: (arrow + 2, body_close),
                    at: i,
                    src,
                });
            }
        }
        dialects.push(dialect);
    }
    // Activities declared in one file are usable from workflows in another
    // only through their own proxy; keep per-file tables but merge for
    // single-directory imports (the common `workflows.ts` + `activities.ts`).
    let mut merged = TemporalDialect {
        activities: HashMap::new(),
        namespaces: HashMap::new(),
    };
    for d in dialects {
        merged.activities.extend(d.activities);
        merged.namespaces.extend(d.namespaces);
    }
    // Only workflows that call a Temporal primitive qualify (activities.ts
    // also exports async functions).
    workflows.retain(|w| (w.body.0..w.body.1).any(|k| merged.classify(w.src, k).is_some()));
    let wf = select(
        &workflows,
        options,
        |w| w.name.as_str(),
        "Temporal workflow (exported async function using proxyActivities/sleep/condition)",
    )?;
    let seq_name = sequence_name(options, &wf.name);
    let mut e = Emit {
        b: Builder::new("temporal", &wf.name, &seq_name),
        src: wf.src,
        vars: HashMap::new(),
    };
    other_candidates(
        &mut e,
        &workflows,
        &wf.name,
        |w| w.name.as_str(),
        "workflow",
    );

    // Workflow arguments: the first one is the instance input.
    let param_ranges = wf.src.split_commas(wf.params.0, wf.params.1);
    if let Some((a, b)) = param_ranges.first().copied() {
        if wf.src.is(a, "{")
            && let Some(close) = wf.src.pair[a]
        {
            for (s, t) in wf.src.split_commas(a + 1, close) {
                if let Some(n) = wf.src.ident(s) {
                    let key = n.to_string();
                    let local = if t > s + 2 && wf.src.is(s + 1, ":") {
                        wf.src.ident(s + 2).unwrap_or(n)
                    } else {
                        n
                    };
                    e.vars.insert(local.to_string(), format!("data.{key}"));
                }
            }
        } else if let Some(n) = wf.src.ident(a) {
            e.vars.insert(n.to_string(), "data".into());
        }
        let _ = b;
    }
    if param_ranges.len() > 1 {
        e.unmapped(wf.at, "workflow arguments", "only the first workflow argument becomes the instance input (context.data); pass the others inside it");
    }
    for (si, text, line) in &proxy_notes {
        if files[*si].path == wf.src.file.path || files.len() > 1 {
            e.b.report.unmapped.push(UnmappedConstruct {
                file: files[*si].path.clone(),
                line: *line,
                construct: text.clone(),
                reason: "no retry.maximumAttempts: Temporal retries activities forever by default; mapped to 10 attempts (1s initial, x2, max 100x initial)".into(),
            });
        }
    }
    for (si, text, line) in &signals {
        e.b.report.unmapped.push(UnmappedConstruct {
            file: files[*si].path.clone(),
            line: *line,
            construct: text.clone(),
            reason: if text == "heartbeatTimeout" {
                "activity heartbeats are not translated — Orch8 workers heartbeat their claimed task instead".into()
            } else {
                "Temporal signal/query/update definitions have no sequence equivalent; use Orch8 signals (POST /instances/{id}/signals) and instance queries".into()
            },
        });
    }

    let parser = Parser {
        src: wf.src,
        dialect: &merged,
        top: wf.body.0,
    };
    let nodes = parser.seq(wf.body.0, wf.body.1);
    let mut handler =
        |e: &mut Emit<'_, '_>, c: &Call| -> Vec<Value> { temporal_call(e, c, &merged) };
    let blocks = e.nodes(&nodes, &mut handler);
    finish(e.b, options, blocks)
}

fn temporal_call(e: &mut Emit<'_, '_>, c: &Call, dialect: &TemporalDialect) -> Vec<Value> {
    let src = e.src;
    let arg = |k: usize| c.args.get(k).map(|(a, b)| src.lit(*a, *b));
    let options = if c.kind == "activity" {
        dialect.activities.get(&c.name).cloned()
    } else {
        c.kind
            .strip_prefix("activity:")
            .and_then(|ns| dialect.namespaces.get(ns).cloned())
    };
    if let Some(opts) = options {
        let handler = snake(&c.name);
        let args: Vec<Value> = (0..c.args.len())
            .filter_map(&arg)
            .collect::<Vec<_>>()
            .iter()
            .map(|l| e.json(l, &format!("{} arguments", c.name)))
            .collect();
        let loc = e.loc(c.at);
        let mut block = e.b.worker_stub(
            &handler,
            "activity",
            &handler,
            json!({ "args": args }),
            &format!("port activity `{}` to the Orch8 worker handler `{handler}` (positional arguments arrive as params.args) — called at {loc}", c.name),
        );
        if let Some(ms) = opts.timeout_ms {
            block["timeout"] = json!(ms);
        }
        if let Some(ms) = opts.deadline_ms {
            block["deadline"] = json!(ms);
        }
        if let Some(retry) = &opts.retry {
            block["retry"] = retry.clone();
        }
        if let Some(queue) = &opts.queue {
            block["queue_name"] = json!(queue);
        }
        let _ = opts.line;
        let id = block["id"].as_str().unwrap_or_default().to_string();
        if let Some(var) = &c.bind {
            e.vars.insert(var.clone(), format!("outputs.{id}"));
        }
        return vec![block];
    }
    match c.kind.as_str() {
        "sleep" => match arg(0).as_ref().and_then(duration_ms) {
            Some(ms) => vec![delay_step(
                e,
                &format!("sleep_l{}", src.line(c.at)),
                "sleep",
                ms,
            )],
            None => vec![todo_step(
                e,
                c,
                "sleep",
                "dynamic sleep duration — Orch8 delays are static",
            )],
        },
        "condition" => {
            let id = e.b.unique_id(&format!("condition_l{}", src.line(c.at)));
            let predicate = c
                .args
                .first()
                .map(|(a, b)| src.text(*a, *b))
                .unwrap_or_default();
            let mut gate = json!({"prompt": format!("Temporal condition: {predicate}")});
            let timeout = arg(1).as_ref().and_then(duration_ms);
            if let Some(ms) = timeout {
                gate["timeout"] = json!(ms);
            }
            e.unmapped(
                c.at,
                &format!("condition({predicate})"),
                &format!(
                    "the predicate over workflow state is not evaluated by Orch8: gate `{id}` waits until \
                     whatever set that state (usually a signal handler) sends the signal `human_input:{id}`{}",
                    if timeout.is_some() { "; on timeout Temporal returns false — the gate is wrapped in try_catch so the run continues" } else { "" }
                ),
            );
            e.b.mapped(&predicate, "condition", &id, "wait_for_input gate");
            let gate_step = json!({"type": "step", "id": id, "handler": "noop", "params": {}, "wait_for_input": gate});
            if timeout.is_some() {
                let tc = e.b.unique_id(&format!("{id}_or_timeout"));
                let noop = e.b.unique_id(&format!("{id}_timed_out"));
                return vec![
                    json!({"type": "try_catch", "id": tc, "try_block": [gate_step], "catch_block": [{"type": "step", "id": noop, "handler": "noop", "params": {}}]}),
                ];
            }
            vec![gate_step]
        }
        "executeChild" => {
            let target = c.args.first().map(|(a, b)| src.lit(*a, *b));
            let child = match &target {
                Some(Lit::Str(s)) => s.clone(),
                Some(Lit::Expr(text, _, _)) => text.clone(),
                _ => {
                    return vec![todo_step(
                        e,
                        c,
                        "executeChild",
                        "child workflow is not a literal reference",
                    )];
                }
            };
            let opts = arg(1).unwrap_or(Lit::Null);
            let input = match opts.get("args") {
                Some(Lit::Arr(items)) if items.len() == 1 => e.json(&items[0], "executeChild args"),
                Some(other) => {
                    let v = e.json(other, "executeChild args");
                    json!({ "args": v })
                }
                None => json!({}),
            };
            if opts.get("workflowId").is_some() {
                e.unmapped(
                    c.at,
                    "executeChild workflowId",
                    "child workflow ids are not translated; Orch8 assigns child instance ids",
                );
            }
            let id = e.b.unique_id(&child);
            let name = snake(&child).replace('_', "-");
            e.b.mapped(&child, "executeChild", &id, &format!("sub_sequence {name}"));
            e.b.report.warnings.push(format!(
                "executeChild `{child}`: import it as sequence `{name}` too"
            ));
            if let Some(var) = &c.bind {
                e.vars.insert(var.clone(), format!("outputs.{id}"));
            }
            vec![json!({"type": "sub_sequence", "id": id, "sequence_name": name, "input": input})]
        }
        "startChild" => vec![todo_step(
            e,
            c,
            "startChild",
            "fire-and-forget child workflow: use `emit_event` to start the child sequence through an event trigger",
        )],
        "continueAsNew" | "makeContinueAsNewFunc" => vec![todo_step(
            e,
            c,
            "continueAsNew",
            "continue-as-new has no equivalent: bound history with a `loop` + `retain_iterations`, or start a new instance via `emit_event`",
        )],
        other => vec![todo_step(e, c, other, "no Orch8 mapping")],
    }
}

// ---------------------------------------------------------------------------
// BullMQ
// ---------------------------------------------------------------------------

struct Flow<'s> {
    root: Lit,
    name: String,
    at: usize,
    src: &'s Src<'s>,
}

/// Convert a `BullMQ` `FlowProducer` tree.
pub fn convert_bullmq(files: &[SourceFile], options: &ConvertOptions) -> Result<Conversion> {
    let srcs: Vec<Src<'_>> = files.iter().map(Src::new).collect();
    let mut flows: Vec<Flow<'_>> = Vec::new();
    let mut standalone: Vec<(usize, usize, String)> = Vec::new();
    let mut workers: Vec<(usize, usize, String)> = Vec::new();
    for (si, src) in srcs.iter().enumerate() {
        let mut producers: Vec<String> = Vec::new();
        let mut queues: Vec<String> = Vec::new();
        for i in 0..src.toks.len() {
            if src.is_ident(i, "new") && src.is(i + 2, "(") {
                let class = src.ident(i + 1).unwrap_or("");
                let var = (i >= 2 && src.is(i - 1, "="))
                    .then(|| src.ident(i - 2))
                    .flatten();
                match class {
                    "FlowProducer" => {
                        if let Some(v) = var {
                            producers.push(v.to_string());
                        }
                    }
                    "Queue" => {
                        if let Some(v) = var {
                            queues.push(v.to_string());
                        }
                    }
                    "Worker" => {
                        let (args, _) = src.args(i + 2);
                        let q = args.first().map(|(a, b)| src.lit(*a, *b));
                        let label = match q {
                            Some(Lit::Str(s)) => s,
                            Some(Lit::Expr(t, _, _)) => t,
                            _ => "?".into(),
                        };
                        workers.push((si, src.line(i), label));
                    }
                    _ => {}
                }
            }
            let Some(method) = src.ident(i + 2) else {
                continue;
            };
            if !(src.is(i + 1, ".") && src.is(i + 3, "(")) {
                continue;
            }
            let Some(var) = src.ident(i) else { continue };
            // `new FlowProducer(...).add(` has `)` before the dot.
            let is_producer = producers.iter().any(|p| p == var);
            let (args, _) = src.args(i + 3);
            if is_producer && matches!(method, "add" | "addBulk") {
                let lit = args.first().map_or(Lit::Null, |(a, b)| src.lit(*a, *b));
                let roots = match (method, lit) {
                    ("addBulk", Lit::Arr(items)) => items,
                    (_, other) => vec![other],
                };
                for root in roots {
                    let name = root
                        .get("name")
                        .and_then(Lit::str)
                        .unwrap_or("flow")
                        .to_string();
                    flows.push(Flow {
                        root,
                        name,
                        at: i,
                        src,
                    });
                }
            } else if queues.iter().any(|q| q == var) && matches!(method, "add" | "addBulk") {
                standalone.push((si, src.line(i), src.text(i, i + 3)));
            }
        }
    }
    let flow = select(
        &flows,
        options,
        |f| f.name.as_str(),
        "BullMQ flow (FlowProducer.add)",
    )?;
    let seq_name = sequence_name(options, &flow.name);
    let mut e = Emit {
        b: Builder::new("bullmq", &flow.name, &seq_name),
        src: flow.src,
        vars: HashMap::new(),
    };
    other_candidates(&mut e, &flows, &flow.name, |f| f.name.as_str(), "flow");
    for (si, line, text) in &standalone {
        e.b.report.unmapped.push(UnmappedConstruct {
            file: files[*si].path.clone(),
            line: *line,
            construct: format!("{text}(...)"),
            reason: "standalone job, not part of a flow: enqueue it with `orch8 job enqueue` (or POST /jobs) instead of a sequence".into(),
        });
    }
    for (si, line, queue) in &workers {
        e.b.report.warnings.push(format!(
            "Worker for queue {queue} ({}:{line}): run it as an Orch8 worker polling the queue's handlers",
            files[*si].path
        ));
    }
    if flow.root.get("children").is_some() {
        e.b.report.warnings.push(
            "a failing child job fails the Orch8 flow (BullMQ's default leaves the parent waiting for \
             its children forever); wrap a child in `try_catch` to keep the parent going"
                .into(),
        );
    }
    let blocks = bullmq_job(&mut e, &flow.root, flow.at);
    finish(e.b, options, blocks)
}

/// A flow job: its children (in parallel) first, then the job itself.
fn bullmq_job(e: &mut Emit<'_, '_>, job: &Lit, at: usize) -> Vec<Value> {
    let name = job
        .get("name")
        .and_then(Lit::str)
        .unwrap_or("job")
        .to_string();
    let mut out = Vec::new();
    if let Some(Lit::Arr(children)) = job.get("children") {
        let branches: Vec<Vec<Value>> = children
            .iter()
            .map(|c| bullmq_job(e, c, at))
            .filter(|b| !b.is_empty())
            .collect();
        match branches.len() {
            0 => {}
            1 => out.extend(branches.into_iter().flatten()),
            _ => {
                let id = e.b.unique_id(&format!("{name}_children"));
                e.b.mapped(&name, "children", &id, "parallel");
                out.push(json!({"type": "parallel", "id": id, "branches": branches}));
            }
        }
    } else if let Some(other) = job.get("children") {
        let text = match other {
            Lit::Expr(t, _, _) => t.clone(),
            _ => "children".into(),
        };
        e.unmapped(
            at,
            &format!("{name}.children = {text}"),
            "children are not a literal array; they were not converted",
        );
    }
    let handler = snake(&name);
    let data = job
        .get("data")
        .cloned()
        .map_or_else(|| json!({}), |d| e.json(&d, &format!("job `{name}` data")));
    let queue = job.get("queueName").and_then(Lit::str).map(str::to_string);
    let reason = format!(
        "port the processor for job `{name}`{} into the Orch8 worker handler `{handler}` (job.data arrives as params)",
        queue
            .as_deref()
            .map(|q| format!(" on queue `{q}`"))
            .unwrap_or_default()
    );
    let mut block =
        e.b.worker_stub(&handler, "flow job", &handler, data, &reason);
    if let Some(q) = &queue {
        block["queue_name"] = json!(q);
    } else {
        e.unmapped(
            at,
            &format!("{name}.queueName"),
            "queueName is not a literal; the step uses the default queue",
        );
    }
    if let Some(opts) = job.get("opts") {
        let attempts = opts.get("attempts").and_then(Lit::num);
        let (initial, multiplier) = match opts.get("backoff") {
            Some(Lit::Num(ms)) => (Some(*ms), 1.0),
            Some(b) => (
                b.get("delay").and_then(Lit::num),
                if b.get("type").and_then(Lit::str) == Some("exponential") {
                    2.0
                } else {
                    1.0
                },
            ),
            None => (None, 1.0),
        };
        if let Some(n) = attempts.filter(|n| *n > 1.0) {
            #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
            let initial = initial.unwrap_or(0.0).max(0.0) as u64;
            #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
            let attempts = n as u64;
            let max = if multiplier > 1.0 {
                initial.saturating_mul(1u64 << attempts.min(20))
            } else {
                initial
            };
            block["retry"] = retry_json(attempts, initial, multiplier, max);
        }
        if let Some(ms) = opts.get("delay").and_then(Lit::num) {
            #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
            let ms = ms.max(0.0) as u64;
            block["delay"] = json!({ "duration": ms });
        }
        if let Lit::Obj(fields) = opts {
            for (key, _, line) in fields {
                let reason = match key.as_str() {
                    "attempts" | "backoff" | "delay" | "removeOnComplete" | "removeOnFail" => {
                        continue;
                    }
                    "jobId" => "custom job ids: use an idempotency key when starting the instance",
                    "failParentOnFailure" => {
                        "Orch8 fails the whole flow when a child fails (BullMQ's default leaves the parent waiting); explicit here, so semantics match"
                    }
                    "ignoreDependencyOnFailure"
                    | "continueParentOnFailure"
                    | "removeDependencyOnFailure" => {
                        "a failing child fails the Orch8 parallel block — wrap the child in `try_catch` to keep the parent going"
                    }
                    "priority" | "lifo" => "set instance priority instead",
                    "repeat" => "repeatable jobs: create an Orch8 cron schedule",
                    _ => "job option not translated",
                };
                e.b.report.unmapped.push(UnmappedConstruct {
                    file: e.src.file.path.clone(),
                    line: *line,
                    construct: format!("{name}.opts.{key}"),
                    reason: reason.into(),
                });
            }
        }
    }
    out.push(block);
    out
}

#[cfg(test)]
#[path = "import_code_tests.rs"]
mod tests;
