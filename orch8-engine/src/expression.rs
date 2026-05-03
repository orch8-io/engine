use std::sync::{Arc, LazyLock};

use orch8_types::context::ExecutionContext;
use tracing::warn;

static TOKEN_CACHE: LazyLock<moka::sync::Cache<String, Arc<[Token]>>> = LazyLock::new(|| {
    moka::sync::Cache::builder()
        .max_capacity(10_000)
        .time_to_idle(std::time::Duration::from_mins(10))
        .build()
});

fn tokenize_cached(input: &str) -> Arc<[Token]> {
    if let Some(cached) = TOKEN_CACHE.get(input) {
        return cached;
    }
    let tokens: Arc<[Token]> = tokenize(input).into();
    TOKEN_CACHE.insert(input.to_owned(), Arc::clone(&tokens));
    tokens
}

/// Expression evaluation error with position information.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExprError {
    pub message: String,
    pub position: usize,
    pub expr: String,
}

impl std::fmt::Display for ExprError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "expression error at position {}: {} in \"{}\"",
            self.position, self.message, self.expr
        )
    }
}

/// Evaluate an expression, returning a detailed error on parse failure.
pub fn try_evaluate(
    expr: &str,
    context: &ExecutionContext,
    outputs: &serde_json::Value,
) -> Result<serde_json::Value, ExprError> {
    let trimmed = expr.trim();
    if trimmed.is_empty() {
        return Ok(serde_json::Value::Null);
    }
    let tokens = tokenize_cached(trimmed);
    let mut parser = Parser::new(&tokens, context, outputs);
    let result = parser.parse_ternary();
    if parser.pos < tokens.len() {
        return Err(ExprError {
            message: format!("unexpected token {:?}", tokens[parser.pos]),
            position: parser.pos,
            expr: expr.to_string(),
        });
    }
    Ok(result)
}

/// Evaluate a simple expression against context and outputs.
///
/// Supported syntax:
/// - Path references: `context.data.x`, `outputs.step_1.result`, `steps.x.y`
/// - Literals: `"hello"`, `42`, `true`, `false`, `null`
/// - Comparisons: `==`, `!=`, `>`, `<`, `>=`, `<=`
/// - Membership: `x in [a, b, c]`
/// - Logical: `&&`, `||`
/// - Arithmetic: `+`, `-`, `*`, `/`
/// - Unary negation: `!expr`
/// - Ternary: `condition ? then : else`
/// - Functions: `abs(x)`, `len(arr)`, `json(obj)`
/// - Array literals: `[1, 2, 3]`
///
/// Returns the evaluated result as a JSON value.
pub fn evaluate(
    expr: &str,
    context: &ExecutionContext,
    outputs: &serde_json::Value,
) -> serde_json::Value {
    let tokens = tokenize_cached(expr.trim());
    let mut parser = Parser::new(&tokens, context, outputs);
    parser.parse_ternary()
}

/// Check if an expression evaluates to a truthy value.
pub fn is_truthy(value: &serde_json::Value) -> bool {
    match value {
        serde_json::Value::Null => false,
        serde_json::Value::Bool(b) => *b,
        serde_json::Value::Number(n) => n.as_f64().is_some_and(|f| f != 0.0),
        serde_json::Value::String(s) => !s.is_empty(),
        serde_json::Value::Array(a) => !a.is_empty(),
        serde_json::Value::Object(_) => true,
    }
}

/// Evaluate a condition string (used by router and loop).
/// Supports the same expression syntax as `evaluate`.
pub fn evaluate_condition(
    condition: &str,
    context: &ExecutionContext,
    outputs: &serde_json::Value,
) -> bool {
    let result = evaluate(condition, context, outputs);
    is_truthy(&result)
}

// === Tokenizer ===

#[derive(Debug, Clone, PartialEq)]
enum Token {
    String(String),
    Number(f64),
    Bool(bool),
    Null,
    Path(String),
    Eq,
    Ne,
    Gt,
    Ge,
    Lt,
    Le,
    And,
    Or,
    Not,
    Plus,
    Minus,
    Star,
    Slash,
    LParen,
    RParen,
    LBracket,
    RBracket,
    Comma,
    Question,
    Colon,
    In,
}

fn tokenize(input: &str) -> Vec<Token> {
    let mut tokens = Vec::new();
    let chars: Vec<char> = input.chars().collect();
    let mut i = 0;

    while i < chars.len() {
        match chars[i] {
            ' ' | '\t' | '\n' | '\r' => i += 1,
            '"' | '\'' => i = tokenize_string(&chars, i, &mut tokens),
            '+' => {
                tokens.push(Token::Plus);
                i += 1;
            }
            '*' => {
                tokens.push(Token::Star);
                i += 1;
            }
            '/' => {
                tokens.push(Token::Slash);
                i += 1;
            }
            '(' => {
                tokens.push(Token::LParen);
                i += 1;
            }
            ')' => {
                tokens.push(Token::RParen);
                i += 1;
            }
            '[' => {
                tokens.push(Token::LBracket);
                i += 1;
            }
            ']' => {
                tokens.push(Token::RBracket);
                i += 1;
            }
            ',' => {
                tokens.push(Token::Comma);
                i += 1;
            }
            '?' => {
                tokens.push(Token::Question);
                i += 1;
            }
            ':' => {
                tokens.push(Token::Colon);
                i += 1;
            }
            '-' => {
                tokens.push(Token::Minus);
                i += 1;
            }
            _ => i = tokenize_complex(&chars, i, &mut tokens),
        }
    }
    tokens
}

fn tokenize_string(chars: &[char], start: usize, tokens: &mut Vec<Token>) -> usize {
    let quote = chars[start];
    let mut i = start + 1;
    let begin = i;
    while i < chars.len() && chars[i] != quote {
        i += 1;
    }
    let s: String = chars[begin..i].iter().collect();
    tokens.push(Token::String(s));
    if i < chars.len() {
        i + 1
    } else {
        i
    }
}

fn tokenize_complex(chars: &[char], i: usize, tokens: &mut Vec<Token>) -> usize {
    let next = if i + 1 < chars.len() {
        Some(chars[i + 1])
    } else {
        None
    };
    match (chars[i], next) {
        ('=', Some('=')) => {
            tokens.push(Token::Eq);
            i + 2
        }
        ('!', Some('=')) => {
            tokens.push(Token::Ne);
            i + 2
        }
        ('>', Some('=')) => {
            tokens.push(Token::Ge);
            i + 2
        }
        ('<', Some('=')) => {
            tokens.push(Token::Le);
            i + 2
        }
        ('&', Some('&')) => {
            tokens.push(Token::And);
            i + 2
        }
        ('|', Some('|')) => {
            tokens.push(Token::Or);
            i + 2
        }
        ('>', _) => {
            tokens.push(Token::Gt);
            i + 1
        }
        ('<', _) => {
            tokens.push(Token::Lt);
            i + 1
        }
        ('!', _) => {
            tokens.push(Token::Not);
            i + 1
        }
        (c, _) if c.is_ascii_digit() => tokenize_number(chars, i, tokens),
        (c, _) if c.is_ascii_alphabetic() || c == '_' => tokenize_word(chars, i, tokens),
        _ => i + 1,
    }
}

fn tokenize_number(chars: &[char], start: usize, tokens: &mut Vec<Token>) -> usize {
    let mut i = start;
    let mut has_dot = false;
    while i < chars.len() && (chars[i].is_ascii_digit() || chars[i] == '.') {
        if chars[i] == '.' {
            if has_dot {
                break; // stop at second dot (e.g. "1.2.3" -> parse "1.2")
            }
            has_dot = true;
        }
        i += 1;
    }
    let num_str: String = chars[start..i].iter().collect();
    if let Ok(n) = num_str.parse::<f64>() {
        if n.is_finite() {
            tokens.push(Token::Number(n));
        }
    }
    i
}

fn tokenize_word(chars: &[char], start: usize, tokens: &mut Vec<Token>) -> usize {
    let mut i = start;
    while i < chars.len()
        && (chars[i].is_ascii_alphanumeric() || chars[i] == '_' || chars[i] == '.')
    {
        i += 1;
    }
    let word: String = chars[start..i].iter().collect();
    tokens.push(match word.as_str() {
        "true" => Token::Bool(true),
        "false" => Token::Bool(false),
        "null" => Token::Null,
        "in" => Token::In,
        _ => Token::Path(word),
    });
    i
}

// === Recursive Descent Parser ===

const MAX_PARSE_DEPTH: u32 = 64;

struct Parser<'a> {
    tokens: &'a [Token],
    pos: usize,
    context: &'a ExecutionContext,
    outputs: &'a serde_json::Value,
    depth: u32,
}

impl<'a> Parser<'a> {
    const fn new(
        tokens: &'a [Token],
        context: &'a ExecutionContext,
        outputs: &'a serde_json::Value,
    ) -> Self {
        Self {
            tokens,
            pos: 0,
            context,
            outputs,
            depth: 0,
        }
    }

    fn peek(&self) -> Option<&Token> {
        self.tokens.get(self.pos)
    }

    fn advance(&mut self) -> Option<&Token> {
        let tok = self.tokens.get(self.pos);
        self.pos += 1;
        tok
    }

    const fn enter(&mut self) -> bool {
        self.depth += 1;
        self.depth <= MAX_PARSE_DEPTH
    }

    const fn leave(&mut self) {
        self.depth -= 1;
    }

    // ternary: or (? or : or)?
    fn parse_ternary(&mut self) -> serde_json::Value {
        if !self.enter() {
            return serde_json::Value::Null;
        }
        let cond = self.parse_or();
        if self.peek() == Some(&Token::Question) {
            self.advance();
            let then_val = self.parse_or();
            if self.peek() == Some(&Token::Colon) {
                self.advance();
            }
            let else_val = self.parse_or();
            self.leave();
            return if is_truthy(&cond) { then_val } else { else_val };
        }
        self.leave();
        cond
    }

    // or: and (|| and)*
    fn parse_or(&mut self) -> serde_json::Value {
        if !self.enter() {
            return serde_json::Value::Null;
        }
        let mut left = self.parse_and();
        while self.peek() == Some(&Token::Or) {
            self.advance();
            let right = self.parse_and();
            left = serde_json::Value::Bool(is_truthy(&left) || is_truthy(&right));
        }
        self.leave();
        left
    }

    // and: comparison (&& comparison)*
    fn parse_and(&mut self) -> serde_json::Value {
        if !self.enter() {
            return serde_json::Value::Null;
        }
        let mut left = self.parse_comparison();
        while self.peek() == Some(&Token::And) {
            self.advance();
            let right = self.parse_comparison();
            left = serde_json::Value::Bool(is_truthy(&left) && is_truthy(&right));
        }
        self.leave();
        left
    }

    // comparison: additive (== | != | > | >= | < | <= additive | in array)?
    fn parse_comparison(&mut self) -> serde_json::Value {
        if !self.enter() {
            return serde_json::Value::Null;
        }
        let left = self.parse_additive();
        let result = match self.peek() {
            Some(Token::Eq) => {
                self.advance();
                let right = self.parse_additive();
                serde_json::Value::Bool(json_eq(&left, &right))
            }
            Some(Token::Ne) => {
                self.advance();
                let right = self.parse_additive();
                serde_json::Value::Bool(!json_eq(&left, &right))
            }
            Some(Token::Gt) => {
                self.advance();
                let right = self.parse_additive();
                serde_json::Value::Bool(
                    json_cmp(&left, &right).is_some_and(|o| o == std::cmp::Ordering::Greater),
                )
            }
            Some(Token::Ge) => {
                self.advance();
                let right = self.parse_additive();
                serde_json::Value::Bool(
                    json_cmp(&left, &right).is_some_and(|o| o != std::cmp::Ordering::Less),
                )
            }
            Some(Token::Lt) => {
                self.advance();
                let right = self.parse_additive();
                serde_json::Value::Bool(
                    json_cmp(&left, &right).is_some_and(|o| o == std::cmp::Ordering::Less),
                )
            }
            Some(Token::Le) => {
                self.advance();
                let right = self.parse_additive();
                serde_json::Value::Bool(
                    json_cmp(&left, &right).is_some_and(|o| o != std::cmp::Ordering::Greater),
                )
            }
            Some(Token::In) => {
                self.advance();
                let right = self.parse_primary();
                let found = match &right {
                    serde_json::Value::Array(arr) => arr.iter().any(|v| json_eq(&left, v)),
                    _ => false,
                };
                serde_json::Value::Bool(found)
            }
            _ => left,
        };
        self.leave();
        result
    }

    // additive: multiplicative ((+ | -) multiplicative)*
    fn parse_additive(&mut self) -> serde_json::Value {
        if !self.enter() {
            return serde_json::Value::Null;
        }
        let mut left = self.parse_multiplicative();
        loop {
            match self.peek() {
                Some(Token::Plus) => {
                    self.advance();
                    let right = self.parse_multiplicative();
                    left = json_add(&left, &right);
                }
                Some(Token::Minus) => {
                    self.advance();
                    let right = self.parse_multiplicative();
                    left = json_sub(&left, &right);
                }
                _ => break,
            }
        }
        self.leave();
        left
    }

    // multiplicative: unary ((* | /) unary)*
    fn parse_multiplicative(&mut self) -> serde_json::Value {
        if !self.enter() {
            return serde_json::Value::Null;
        }
        let mut left = self.parse_unary();
        loop {
            match self.peek() {
                Some(Token::Star) => {
                    self.advance();
                    let right = self.parse_unary();
                    left = json_mul(&left, &right);
                }
                Some(Token::Slash) => {
                    self.advance();
                    let right = self.parse_unary();
                    left = json_div(&left, &right);
                }
                _ => break,
            }
        }
        self.leave();
        left
    }

    // unary: !unary | -unary | primary
    fn parse_unary(&mut self) -> serde_json::Value {
        if !self.enter() {
            return serde_json::Value::Null;
        }
        if self.peek() == Some(&Token::Not) {
            self.advance();
            let val = self.parse_unary();
            self.leave();
            return serde_json::Value::Bool(!is_truthy(&val));
        }
        if self.peek() == Some(&Token::Minus) {
            self.advance();
            let val = self.parse_unary();
            self.leave();
            return if let Some(n) = to_f64(&val) {
                serde_json::json!(-n)
            } else {
                warn!(
                    value = %val,
                    "expression arithmetic: non-numeric operand in negation"
                );
                serde_json::Value::Null
            };
        }
        let result = self.parse_primary();
        self.leave();
        result
    }

    // primary: literal | path | function(expr) | (expr) | [expr, ...]
    fn parse_primary(&mut self) -> serde_json::Value {
        match self.advance().cloned() {
            Some(Token::String(s)) => serde_json::Value::String(s),
            Some(Token::Number(n)) => serde_json::json!(n),
            Some(Token::Bool(b)) => serde_json::Value::Bool(b),
            Some(Token::Path(path)) => {
                if self.peek() == Some(&Token::LParen) {
                    self.advance(); // consume (
                    let mut args = Vec::new();
                    if self.peek() != Some(&Token::RParen) {
                        args.push(self.parse_ternary());
                        while self.peek() == Some(&Token::Comma) {
                            self.advance();
                            if self.peek() == Some(&Token::RParen) {
                                break;
                            }
                            args.push(self.parse_ternary());
                        }
                    }
                    if self.peek() == Some(&Token::RParen) {
                        self.advance(); // consume )
                    }
                    return Self::apply_function(&path, &args);
                }
                self.resolve_path(&path)
            }
            Some(Token::LParen) => {
                let val = self.parse_ternary();
                if self.peek() == Some(&Token::RParen) {
                    self.advance();
                }
                val
            }
            Some(Token::LBracket) => {
                // Array literal: [expr, expr, ...]
                let mut items = Vec::new();
                if self.peek() != Some(&Token::RBracket) {
                    items.push(self.parse_ternary());
                    while self.peek() == Some(&Token::Comma) {
                        self.advance();
                        if self.peek() == Some(&Token::RBracket) {
                            break; // trailing comma
                        }
                        items.push(self.parse_ternary());
                    }
                }
                if self.peek() == Some(&Token::RBracket) {
                    self.advance();
                }
                serde_json::Value::Array(items)
            }
            _ => serde_json::Value::Null,
        }
    }

    #[allow(
        clippy::too_many_lines,
        clippy::cast_possible_truncation,
        clippy::cast_sign_loss,
        clippy::cast_precision_loss
    )]
    fn apply_function(name: &str, args: &[serde_json::Value]) -> serde_json::Value {
        let arg = args.first().unwrap_or(&serde_json::Value::Null);
        match name {
            "abs" => match to_f64(arg) {
                Some(n) => serde_json::json!(n.abs()),
                None => serde_json::Value::Null,
            },
            "len" => {
                let n = match arg {
                    serde_json::Value::Array(a) => a.len(),
                    serde_json::Value::Object(m) => m.len(),
                    serde_json::Value::String(s) => s.len(),
                    _ => 0,
                };
                serde_json::json!(n)
            }
            "json" => serde_json::Value::String(arg.to_string()),
            // --- template-enhancements-v0.3.1 ---
            "now" => serde_json::json!(chrono::Utc::now().to_rfc3339()),
            "uuid" => serde_json::json!(uuid::Uuid::new_v4().to_string()),
            "random" => {
                let min = args.first().and_then(to_f64).unwrap_or(0.0) as i64;
                let max = args.get(1).and_then(to_f64).unwrap_or(100.0) as i64;
                if max <= min {
                    return serde_json::json!(min);
                }
                let range = (max - min) as u64;
                #[allow(clippy::cast_possible_wrap)]
                let n = (rand::random::<u64>() % range) as i64 + min;
                serde_json::json!(n)
            }
            "format_date" => {
                let iso = arg.as_str().unwrap_or("");
                let fmt = args.get(1).and_then(|v| v.as_str()).unwrap_or("%Y-%m-%d");
                chrono::DateTime::parse_from_rfc3339(iso).map_or(serde_json::Value::Null, |dt| {
                    serde_json::json!(dt.format(fmt).to_string())
                })
            }
            "day_of_week" => {
                let iso = arg.as_str().unwrap_or("");
                chrono::DateTime::parse_from_rfc3339(iso).map_or(serde_json::Value::Null, |dt| {
                    use chrono::Datelike;
                    serde_json::json!(dt.weekday().num_days_from_monday())
                })
            }
            "keys" => match arg {
                serde_json::Value::Object(m) => {
                    serde_json::json!(m.keys().collect::<Vec<_>>())
                }
                _ => serde_json::json!([]),
            },
            "values" => match arg {
                serde_json::Value::Object(m) => {
                    serde_json::Value::Array(m.values().cloned().collect())
                }
                _ => serde_json::json!([]),
            },
            "contains" => {
                let needle = args.get(1).unwrap_or(&serde_json::Value::Null);
                let found = match arg {
                    serde_json::Value::Array(a) => a.iter().any(|v| json_eq(v, needle)),
                    serde_json::Value::String(s) => needle.as_str().is_some_and(|n| s.contains(n)),
                    _ => false,
                };
                serde_json::Value::Bool(found)
            }
            "starts_with" => {
                let prefix = args.get(1).and_then(|v| v.as_str()).unwrap_or("");
                let s = arg.as_str().unwrap_or("");
                serde_json::Value::Bool(s.starts_with(prefix))
            }
            "ends_with" => {
                let suffix = args.get(1).and_then(|v| v.as_str()).unwrap_or("");
                let s = arg.as_str().unwrap_or("");
                serde_json::Value::Bool(s.ends_with(suffix))
            }
            // --- engine-enhancements: array/math ---
            "sum" => match arg {
                serde_json::Value::Array(a) => {
                    let s: f64 = a.iter().filter_map(to_f64).sum();
                    serde_json::json!(s)
                }
                _ => serde_json::Value::Null,
            },
            "avg" => match arg {
                serde_json::Value::Array(a) => {
                    let nums: Vec<f64> = a.iter().filter_map(to_f64).collect();
                    if nums.is_empty() {
                        serde_json::Value::Null
                    } else {
                        let s: f64 = nums.iter().sum();
                        serde_json::json!(s / nums.len() as f64)
                    }
                }
                _ => serde_json::Value::Null,
            },
            "min" => match arg {
                serde_json::Value::Array(a) => a
                    .iter()
                    .filter_map(to_f64)
                    .reduce(f64::min)
                    .map_or(serde_json::Value::Null, |v| serde_json::json!(v)),
                _ => serde_json::Value::Null,
            },
            "max" => match arg {
                serde_json::Value::Array(a) => a
                    .iter()
                    .filter_map(to_f64)
                    .reduce(f64::max)
                    .map_or(serde_json::Value::Null, |v| serde_json::json!(v)),
                _ => serde_json::Value::Null,
            },
            "first" => match arg {
                serde_json::Value::Array(a) => {
                    a.first().cloned().unwrap_or(serde_json::Value::Null)
                }
                _ => serde_json::Value::Null,
            },
            "last" => match arg {
                serde_json::Value::Array(a) => a.last().cloned().unwrap_or(serde_json::Value::Null),
                _ => serde_json::Value::Null,
            },
            "slice" => {
                let start = args.get(1).and_then(to_f64).unwrap_or(0.0) as usize;
                let end = args.get(2).and_then(to_f64);
                match arg {
                    serde_json::Value::Array(a) => {
                        let end_idx = end.map_or(a.len(), |e| (e as usize).min(a.len()));
                        let start_idx = start.min(a.len());
                        serde_json::Value::Array(a[start_idx..end_idx].to_vec())
                    }
                    _ => serde_json::Value::Null,
                }
            }
            "sort" => {
                let order = args.get(1).and_then(|v| v.as_str()).unwrap_or("asc");
                match arg {
                    serde_json::Value::Array(a) => {
                        let mut sorted = a.clone();
                        sorted.sort_by(|a, b| {
                            let cmp = json_cmp(a, b).unwrap_or(std::cmp::Ordering::Equal);
                            if order == "desc" {
                                cmp.reverse()
                            } else {
                                cmp
                            }
                        });
                        serde_json::Value::Array(sorted)
                    }
                    _ => serde_json::Value::Null,
                }
            }
            "unique" => match arg {
                serde_json::Value::Array(a) => {
                    let mut seen = Vec::new();
                    for v in a {
                        if !seen.iter().any(|s| json_eq(s, v)) {
                            seen.push(v.clone());
                        }
                    }
                    serde_json::Value::Array(seen)
                }
                _ => serde_json::Value::Null,
            },
            "count" => {
                let needle = args.get(1).unwrap_or(&serde_json::Value::Null);
                match arg {
                    serde_json::Value::Array(a) => {
                        let n = a.iter().filter(|v| json_eq(v, needle)).count();
                        serde_json::json!(n)
                    }
                    _ => serde_json::json!(0),
                }
            }
            "change_pct" => {
                let old = to_f64(arg);
                let new = args.get(1).and_then(to_f64);
                match (old, new) {
                    (Some(o), Some(n)) if o != 0.0 => serde_json::json!((n - o) / o * 100.0),
                    _ => serde_json::Value::Null,
                }
            }
            "clamp" => {
                let val = to_f64(arg);
                let min = args.get(1).and_then(to_f64);
                let max = args.get(2).and_then(to_f64);
                match (val, min, max) {
                    (Some(v), Some(lo), Some(hi)) => serde_json::json!(v.clamp(lo, hi)),
                    _ => arg.clone(),
                }
            }
            _ => serde_json::Value::Null,
        }
    }

    fn resolve_path(&self, path: &str) -> serde_json::Value {
        let parts: Vec<&str> = path.split('.').collect();
        match parts.first().copied() {
            Some("context") => {
                let section = parts.get(1).copied().unwrap_or("");
                let source = match section {
                    "data" => &self.context.data,
                    "config" => &self.context.config,
                    _ => return serde_json::Value::Null,
                };
                navigate_json(source, &parts[2..])
            }
            Some("outputs" | "steps") => navigate_json(self.outputs, &parts[1..]),
            Some("input") => {
                let mut full = vec!["input"];
                full.extend_from_slice(&parts[1..]);
                navigate_json(&self.context.data, &full)
            }
            Some("instance_id") => {
                let id = self.context.runtime.instance_id.as_deref().unwrap_or("");
                serde_json::Value::String(id.to_string())
            }
            Some("config") => navigate_json(&self.context.config, &parts[1..]),
            Some("data") => navigate_json(&self.context.data, &parts[1..]),
            Some("runtime") => {
                let rt_json =
                    serde_json::to_value(&self.context.runtime).unwrap_or(serde_json::Value::Null);
                navigate_json(&rt_json, &parts[1..])
            }
            _ => {
                // Bare path — resolve from context.data for backwards compatibility.
                navigate_json(&self.context.data, &parts)
            }
        }
    }
}

fn navigate_json(value: &serde_json::Value, path: &[&str]) -> serde_json::Value {
    let mut current = value;
    for &segment in path {
        match current {
            serde_json::Value::Object(map) => match map.get(segment) {
                Some(v) => current = v,
                None => return serde_json::Value::Null,
            },
            serde_json::Value::Array(arr) => match segment.parse::<usize>() {
                Ok(idx) => match arr.get(idx) {
                    Some(v) => current = v,
                    None => return serde_json::Value::Null,
                },
                Err(_) => return serde_json::Value::Null,
            },
            _ => return serde_json::Value::Null,
        }
    }
    current.clone()
}

// === JSON value operations ===

fn json_eq(a: &serde_json::Value, b: &serde_json::Value) -> bool {
    match (a, b) {
        (serde_json::Value::Number(a), serde_json::Value::Number(b)) => a.as_f64() == b.as_f64(),
        _ => a == b,
    }
}

fn json_cmp(a: &serde_json::Value, b: &serde_json::Value) -> Option<std::cmp::Ordering> {
    match (a, b) {
        (serde_json::Value::Number(a), serde_json::Value::Number(b)) => {
            a.as_f64()?.partial_cmp(&b.as_f64()?)
        }
        (serde_json::Value::String(a), serde_json::Value::String(b)) => Some(a.cmp(b)),
        _ => None,
    }
}

fn to_f64(v: &serde_json::Value) -> Option<f64> {
    let f = match v {
        serde_json::Value::Number(n) => n.as_f64()?,
        serde_json::Value::String(s) => s.parse().ok()?,
        serde_json::Value::Bool(b) => {
            if *b {
                1.0
            } else {
                0.0
            }
        }
        _ => return None,
    };
    if f.is_finite() {
        Some(f)
    } else {
        None
    }
}

fn json_add(a: &serde_json::Value, b: &serde_json::Value) -> serde_json::Value {
    // String concatenation if either side is a string.
    if let (serde_json::Value::String(a), serde_json::Value::String(b)) = (a, b) {
        return serde_json::Value::String(format!("{a}{b}"));
    }
    if let (Some(a), Some(b)) = (to_f64(a), to_f64(b)) {
        serde_json::json!(a + b)
    } else {
        warn!(
            left = %a,
            right = %b,
            "expression arithmetic: non-numeric operand in addition"
        );
        serde_json::Value::Null
    }
}

fn json_sub(a: &serde_json::Value, b: &serde_json::Value) -> serde_json::Value {
    if let (Some(a), Some(b)) = (to_f64(a), to_f64(b)) {
        serde_json::json!(a - b)
    } else {
        warn!(
            left = %a,
            right = %b,
            "expression arithmetic: non-numeric operand in subtraction"
        );
        serde_json::Value::Null
    }
}

fn json_mul(a: &serde_json::Value, b: &serde_json::Value) -> serde_json::Value {
    if let (Some(a), Some(b)) = (to_f64(a), to_f64(b)) {
        serde_json::json!(a * b)
    } else {
        warn!(
            left = %a,
            right = %b,
            "expression arithmetic: non-numeric operand in multiplication"
        );
        serde_json::Value::Null
    }
}

fn json_div(a: &serde_json::Value, b: &serde_json::Value) -> serde_json::Value {
    match (to_f64(a), to_f64(b)) {
        (Some(a), Some(b)) if b != 0.0 => serde_json::json!(a / b),
        _ => {
            if to_f64(a).is_some() && (to_f64(b) == Some(0.0)) {
                warn!(
                    left = %a,
                    right = %b,
                    "expression arithmetic: division by zero"
                );
            } else {
                warn!(
                    left = %a,
                    right = %b,
                    "expression arithmetic: non-numeric operand in division"
                );
            }
            serde_json::Value::Null
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use orch8_types::context::ExecutionContext;
    use serde_json::json;

    fn ctx() -> ExecutionContext {
        ExecutionContext {
            data: json!({"user": {"name": "Alice", "age": 30}, "count": 5, "active": true}),
            config: json!({"limit": 100}),
            ..Default::default()
        }
    }

    fn outputs() -> serde_json::Value {
        json!({"step_1": {"result": "ok", "count": 42}})
    }

    #[test]
    fn eval_path() {
        assert_eq!(
            evaluate("context.data.user.name", &ctx(), &outputs()),
            json!("Alice")
        );
        assert_eq!(evaluate("context.data.count", &ctx(), &outputs()), json!(5));
    }

    #[test]
    fn eval_bare_path() {
        assert_eq!(evaluate("user.name", &ctx(), &outputs()), json!("Alice"));
        assert_eq!(evaluate("count", &ctx(), &outputs()), json!(5));
    }

    #[test]
    fn eval_outputs_path() {
        assert_eq!(
            evaluate("outputs.step_1.count", &ctx(), &outputs()),
            json!(42)
        );
    }

    #[test]
    fn eval_equality() {
        assert_eq!(
            evaluate("context.data.count == 5", &ctx(), &outputs()),
            json!(true)
        );
        assert_eq!(
            evaluate("context.data.count != 5", &ctx(), &outputs()),
            json!(false)
        );
        assert_eq!(
            evaluate("context.data.user.name == \"Alice\"", &ctx(), &outputs()),
            json!(true)
        );
    }

    #[test]
    fn eval_comparison() {
        assert_eq!(
            evaluate("context.data.count > 3", &ctx(), &outputs()),
            json!(true)
        );
        assert_eq!(
            evaluate("context.data.count < 3", &ctx(), &outputs()),
            json!(false)
        );
        assert_eq!(
            evaluate("context.data.count >= 5", &ctx(), &outputs()),
            json!(true)
        );
        assert_eq!(
            evaluate("context.data.count <= 5", &ctx(), &outputs()),
            json!(true)
        );
    }

    #[test]
    fn eval_arithmetic() {
        assert_eq!(
            evaluate("context.data.count + 10", &ctx(), &outputs()),
            json!(15.0)
        );
        assert_eq!(
            evaluate("context.data.count * 2", &ctx(), &outputs()),
            json!(10.0)
        );
        assert_eq!(
            evaluate("outputs.step_1.count - 2", &ctx(), &outputs()),
            json!(40.0)
        );
        assert_eq!(evaluate("10 / 3", &ctx(), &outputs()), json!(10.0 / 3.0));
    }

    #[test]
    fn eval_logical() {
        assert_eq!(
            evaluate(
                "context.data.active && context.data.count > 3",
                &ctx(),
                &outputs()
            ),
            json!(true)
        );
        assert_eq!(
            evaluate(
                "context.data.active && context.data.count > 10",
                &ctx(),
                &outputs()
            ),
            json!(false)
        );
        assert_eq!(
            evaluate("false || context.data.active", &ctx(), &outputs()),
            json!(true)
        );
    }

    #[test]
    fn eval_not() {
        assert_eq!(evaluate("!false", &ctx(), &outputs()), json!(true));
        assert_eq!(
            evaluate("!context.data.active", &ctx(), &outputs()),
            json!(false)
        );
    }

    #[test]
    fn eval_string_concat() {
        assert_eq!(
            evaluate("\"hello\" + \" world\"", &ctx(), &outputs()),
            json!("hello world")
        );
    }

    #[test]
    fn eval_parentheses() {
        assert_eq!(evaluate("(2 + 3) * 4", &ctx(), &outputs()), json!(20.0));
    }

    #[test]
    fn eval_literals() {
        assert_eq!(evaluate("42", &ctx(), &outputs()), json!(42.0));
        assert_eq!(evaluate("true", &ctx(), &outputs()), json!(true));
        assert_eq!(evaluate("null", &ctx(), &outputs()), json!(null));
        assert_eq!(evaluate("\"hello\"", &ctx(), &outputs()), json!("hello"));
    }

    #[test]
    fn eval_missing_path() {
        assert_eq!(
            evaluate("context.data.nonexistent", &ctx(), &outputs()),
            json!(null)
        );
    }

    #[test]
    fn is_truthy_values() {
        assert!(is_truthy(&json!(true)));
        assert!(is_truthy(&json!(1)));
        assert!(is_truthy(&json!("hello")));
        assert!(!is_truthy(&json!(null)));
        assert!(!is_truthy(&json!(false)));
        assert!(!is_truthy(&json!(0)));
        assert!(!is_truthy(&json!("")));
    }

    #[test]
    fn eval_deeply_nested_path() {
        let ctx = ExecutionContext {
            data: json!({"a": {"b": {"c": {"d": "found"}}}}),
            config: json!({}),
            ..Default::default()
        };
        assert_eq!(
            evaluate("context.data.a.b.c.d", &ctx, &json!({})),
            json!("found")
        );
    }

    #[test]
    fn eval_string_literal() {
        assert_eq!(evaluate("\"hello\"", &ctx(), &outputs()), json!("hello"));
    }

    #[test]
    fn eval_number_literal() {
        assert_eq!(evaluate("42", &ctx(), &outputs()), json!(42.0));
    }

    #[test]
    fn eval_bool_literal_true() {
        assert_eq!(evaluate("true", &ctx(), &outputs()), json!(true));
    }

    #[test]
    fn eval_null_literal() {
        assert_eq!(evaluate("null", &ctx(), &outputs()), json!(null));
    }

    #[test]
    fn eval_logical_and_false() {
        assert_eq!(evaluate("true && false", &ctx(), &outputs()), json!(false));
    }

    #[test]
    fn eval_logical_or_true() {
        assert_eq!(evaluate("false || true", &ctx(), &outputs()), json!(true));
    }

    #[test]
    fn eval_arithmetic_add_literal() {
        assert_eq!(evaluate("3 + 4", &ctx(), &outputs()), json!(7.0));
    }

    #[test]
    fn eval_arithmetic_sub_literal() {
        assert_eq!(evaluate("10 - 3", &ctx(), &outputs()), json!(7.0));
    }

    #[test]
    fn eval_arithmetic_mul_literal() {
        assert_eq!(evaluate("3 * 4", &ctx(), &outputs()), json!(12.0));
    }

    #[test]
    fn eval_operator_precedence() {
        assert_eq!(evaluate("2 + 3 * 4", &ctx(), &outputs()), json!(14.0));
    }

    #[test]
    fn eval_string_equality() {
        assert_eq!(
            evaluate("\"abc\" == \"abc\"", &ctx(), &outputs()),
            json!(true)
        );
    }

    #[test]
    fn eval_empty_expression_returns_null() {
        assert_eq!(evaluate("", &ctx(), &outputs()), json!(null));
    }

    // --- Tokenizer edge cases ---

    #[test]
    fn eval_single_quote_string() {
        assert_eq!(evaluate("'hello'", &ctx(), &outputs()), json!("hello"));
    }

    #[test]
    fn eval_whitespace_only_returns_null() {
        assert_eq!(evaluate("   ", &ctx(), &outputs()), json!(null));
    }

    #[test]
    fn eval_unterminated_string_still_parses() {
        // Unterminated string should capture up to end.
        let result = evaluate("\"hello", &ctx(), &outputs());
        assert_eq!(result, json!("hello"));
    }

    #[test]
    fn eval_number_with_decimal() {
        #[allow(clippy::approx_constant)]
        let expected = json!(3.14);
        assert_eq!(evaluate("3.14", &ctx(), &outputs()), expected);
    }

    #[test]
    fn eval_number_with_two_dots_stops_at_second() {
        // "1.2.3" should tokenize as 1.2, then path "3"
        let result = evaluate("1.2", &ctx(), &outputs());
        assert_eq!(result, json!(1.2));
    }

    // --- Parser edge cases ---

    #[test]
    fn eval_division_by_zero_returns_null() {
        assert_eq!(evaluate("10 / 0", &ctx(), &outputs()), json!(null));
    }

    #[test]
    fn eval_nested_parentheses() {
        assert_eq!(evaluate("((2 + 3))", &ctx(), &outputs()), json!(5.0));
    }

    #[test]
    fn eval_chained_comparisons_left_associative() {
        // "5 > 3" evaluates first to true, then "true > 1" which is not comparable
        // so the comparison part doesn't chain — just "5 > 3" → true
        assert_eq!(evaluate("5 > 3", &ctx(), &outputs()), json!(true));
    }

    #[test]
    fn eval_not_not_double_negation() {
        assert_eq!(evaluate("!!true", &ctx(), &outputs()), json!(true));
        assert_eq!(evaluate("!!false", &ctx(), &outputs()), json!(false));
    }

    #[test]
    fn eval_not_null_is_true() {
        assert_eq!(evaluate("!null", &ctx(), &outputs()), json!(true));
    }

    #[test]
    fn eval_not_zero_is_true() {
        assert_eq!(evaluate("!0", &ctx(), &outputs()), json!(true));
    }

    #[test]
    fn eval_not_nonzero_is_false() {
        assert_eq!(evaluate("!1", &ctx(), &outputs()), json!(false));
    }

    // --- Path resolution ---

    #[test]
    fn eval_config_path() {
        assert_eq!(
            evaluate("context.config.limit", &ctx(), &outputs()),
            json!(100)
        );
    }

    #[test]
    fn eval_invalid_context_section_returns_null() {
        assert_eq!(
            evaluate("context.unknown.field", &ctx(), &outputs()),
            json!(null)
        );
    }

    #[test]
    fn eval_context_data_array_index() {
        let ctx = ExecutionContext {
            data: json!({"items": [10, 20, 30]}),
            config: json!({}),
            ..Default::default()
        };
        assert_eq!(evaluate("items.1", &ctx, &json!({})), json!(20));
    }

    #[test]
    fn eval_context_data_array_out_of_bounds() {
        let ctx = ExecutionContext {
            data: json!({"items": [10]}),
            config: json!({}),
            ..Default::default()
        };
        assert_eq!(evaluate("items.5", &ctx, &json!({})), json!(null));
    }

    #[test]
    fn eval_path_through_non_object_returns_null() {
        let ctx = ExecutionContext {
            data: json!({"x": 42}),
            config: json!({}),
            ..Default::default()
        };
        assert_eq!(evaluate("x.nested", &ctx, &json!({})), json!(null));
    }

    // --- Arithmetic edge cases ---

    #[test]
    fn eval_string_plus_string_concatenates() {
        assert_eq!(
            evaluate("\"foo\" + \"bar\"", &ctx(), &outputs()),
            json!("foobar")
        );
    }

    #[test]
    fn eval_null_arithmetic_returns_null() {
        assert_eq!(evaluate("null + 1", &ctx(), &outputs()), json!(null));
        assert_eq!(evaluate("null - 1", &ctx(), &outputs()), json!(null));
        assert_eq!(evaluate("null * 1", &ctx(), &outputs()), json!(null));
        assert_eq!(evaluate("null / 1", &ctx(), &outputs()), json!(null));
    }

    #[test]
    fn eval_bool_to_number_coercion() {
        assert_eq!(evaluate("true + 1", &ctx(), &outputs()), json!(2.0));
        assert_eq!(evaluate("false + 1", &ctx(), &outputs()), json!(1.0));
    }

    #[test]
    fn eval_string_to_number_coercion_in_arithmetic() {
        // String "3" should coerce to 3.0 in numeric context.
        let ctx = ExecutionContext {
            data: json!({"val": "3"}),
            config: json!({}),
            ..Default::default()
        };
        assert_eq!(evaluate("val + 2", &ctx, &json!({})), json!(5.0));
    }

    // --- Comparison edge cases ---

    #[test]
    fn eval_string_comparison() {
        assert_eq!(
            evaluate("\"abc\" > \"aaa\"", &ctx(), &outputs()),
            json!(true)
        );
        assert_eq!(
            evaluate("\"abc\" < \"abd\"", &ctx(), &outputs()),
            json!(true)
        );
    }

    #[test]
    fn eval_incompatible_comparison_returns_false() {
        // Number vs string comparison should return false (None ordering).
        assert_eq!(evaluate("42 > \"hello\"", &ctx(), &outputs()), json!(false));
    }

    #[test]
    fn eval_null_equality() {
        assert_eq!(evaluate("null == null", &ctx(), &outputs()), json!(true));
        assert_eq!(evaluate("null != null", &ctx(), &outputs()), json!(false));
    }

    // --- is_truthy edge cases ---

    #[test]
    fn is_truthy_empty_object_is_truthy() {
        assert!(is_truthy(&json!({})));
    }

    #[test]
    fn is_truthy_negative_number_is_truthy() {
        assert!(is_truthy(&json!(-1)));
    }

    #[test]
    fn is_truthy_float_zero_is_falsy() {
        assert!(!is_truthy(&json!(0.0)));
    }

    #[test]
    fn is_truthy_whitespace_string_is_truthy() {
        assert!(is_truthy(&json!(" ")));
    }

    #[test]
    fn condition_evaluation() {
        assert!(evaluate_condition(
            "context.data.active",
            &ctx(),
            &outputs()
        ));
        assert!(evaluate_condition(
            "context.data.count > 3",
            &ctx(),
            &outputs()
        ));
        assert!(!evaluate_condition(
            "context.data.count > 10",
            &ctx(),
            &outputs()
        ));
    }

    #[test]
    fn try_evaluate_valid_expression_returns_ok() {
        let result = try_evaluate("context.data.count + 1", &ctx(), &outputs());
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), json!(6.0));
    }

    #[test]
    fn try_evaluate_empty_returns_null() {
        let result = try_evaluate("", &ctx(), &outputs());
        assert_eq!(result.unwrap(), json!(null));
    }

    #[test]
    fn try_evaluate_error_includes_position() {
        // "5 5" — the second 5 is unconsumed
        let result = try_evaluate("5 5", &ctx(), &outputs());
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(err.message.contains("unexpected token"));
        assert_eq!(err.position, 1);
    }

    #[test]
    fn try_evaluate_error_display_includes_expr() {
        let result = try_evaluate("5 5", &ctx(), &outputs());
        let err = result.unwrap_err();
        let display = err.to_string();
        assert!(display.contains("position 1"), "got: {display}");
        assert!(display.contains("5 5"), "got: {display}");
    }

    // ------------------------------------------------------------------
    // tokenize_cached
    // ------------------------------------------------------------------

    #[test]
    fn tokenize_cached_returns_same_tokens_as_tokenize() {
        let inputs = [
            "context.data.user.name",
            "1 + 2 * 3",
            "'hello' + ' ' + 'world'",
            "true && false || !true",
            "context.data.age >= 18",
            "",
        ];
        for input in inputs {
            let direct: Vec<Token> = tokenize(input);
            let cached = tokenize_cached(input);
            assert_eq!(&direct[..], &cached[..], "mismatch for input: {input:?}");
        }
    }

    #[test]
    fn tokenize_cached_returns_arc_clone_on_cache_hit() {
        let expr = "context.data.x == 42";
        let first = tokenize_cached(expr);
        let second = tokenize_cached(expr);
        assert!(
            Arc::ptr_eq(&first, &second),
            "second call should return the same Arc (cache hit)"
        );
    }

    #[test]
    fn tokenize_cached_distinct_inputs_produce_distinct_entries() {
        let a = tokenize_cached("1 + 2");
        let b = tokenize_cached("3 + 4");
        assert!(!Arc::ptr_eq(&a, &b));
        assert_ne!(&a[..], &b[..]);
    }

    // --- steps alias ---

    #[test]
    fn eval_steps_alias() {
        assert_eq!(
            evaluate("steps.step_1.count", &ctx(), &outputs()),
            json!(42)
        );
    }

    #[test]
    fn eval_steps_alias_comparison() {
        assert_eq!(
            evaluate("steps.step_1.count == 42", &ctx(), &outputs()),
            json!(true)
        );
    }

    // --- in operator ---

    #[test]
    fn eval_in_array_literal_found() {
        assert_eq!(
            evaluate("'strong' in ['strong', 'moderate']", &ctx(), &outputs()),
            json!(true)
        );
    }

    #[test]
    fn eval_in_array_literal_not_found() {
        assert_eq!(
            evaluate("'weak' in ['strong', 'moderate']", &ctx(), &outputs()),
            json!(false)
        );
    }

    #[test]
    fn eval_in_number_array() {
        assert_eq!(
            evaluate("5 in [1, 2, 3, 5, 8]", &ctx(), &outputs()),
            json!(true)
        );
    }

    #[test]
    fn eval_in_with_path() {
        let ctx = ExecutionContext {
            data: json!({"level": "moderate"}),
            config: json!({}),
            ..Default::default()
        };
        assert_eq!(
            evaluate("level in ['strong', 'moderate']", &ctx, &json!({})),
            json!(true)
        );
    }

    #[test]
    fn eval_in_empty_array() {
        assert_eq!(evaluate("'x' in []", &ctx(), &outputs()), json!(false));
    }

    // --- ternary operator ---

    #[test]
    fn eval_ternary_true() {
        assert_eq!(
            evaluate("true ? 'yes' : 'no'", &ctx(), &outputs()),
            json!("yes")
        );
    }

    #[test]
    fn eval_ternary_false() {
        assert_eq!(
            evaluate("false ? 'yes' : 'no'", &ctx(), &outputs()),
            json!("no")
        );
    }

    #[test]
    fn eval_ternary_with_expression() {
        assert_eq!(
            evaluate(
                "context.data.count > 3 ? 'big' : 'small'",
                &ctx(),
                &outputs()
            ),
            json!("big")
        );
    }

    #[test]
    fn eval_ternary_with_numbers() {
        assert_eq!(evaluate("true ? 1 : 0", &ctx(), &outputs()), json!(1.0));
    }

    // --- abs() function ---

    #[test]
    fn eval_abs_positive() {
        assert_eq!(evaluate("abs(5)", &ctx(), &outputs()), json!(5.0));
    }

    #[test]
    fn eval_abs_negative() {
        assert_eq!(evaluate("abs(-5)", &ctx(), &outputs()), json!(5.0));
    }

    #[test]
    fn eval_abs_expression() {
        assert_eq!(evaluate("abs(3 - 10)", &ctx(), &outputs()), json!(7.0));
    }

    #[test]
    fn eval_abs_null_returns_null() {
        assert_eq!(evaluate("abs(null)", &ctx(), &outputs()), json!(null));
    }

    // --- len() function ---

    #[test]
    fn eval_len_array() {
        let ctx = ExecutionContext {
            data: json!({"items": [1, 2, 3]}),
            config: json!({}),
            ..Default::default()
        };
        assert_eq!(evaluate("len(items)", &ctx, &json!({})), json!(3));
    }

    #[test]
    fn eval_len_string() {
        assert_eq!(evaluate("len('hello')", &ctx(), &outputs()), json!(5));
    }

    #[test]
    fn eval_len_empty_array() {
        let ctx = ExecutionContext {
            data: json!({"items": []}),
            config: json!({}),
            ..Default::default()
        };
        assert_eq!(evaluate("len(items)", &ctx, &json!({})), json!(0));
    }

    // --- json() function ---

    #[test]
    fn eval_json_object() {
        let outputs = json!({"step_1": {"data": {"x": 1}}});
        assert_eq!(
            evaluate("json(steps.step_1.data)", &ctx(), &outputs),
            json!("{\"x\":1}")
        );
    }

    // --- array literals ---

    #[test]
    fn eval_array_literal() {
        let result = evaluate("[1, 2, 3]", &ctx(), &outputs());
        assert_eq!(result, json!([1.0, 2.0, 3.0]));
    }

    #[test]
    fn eval_array_literal_strings() {
        let result = evaluate("['a', 'b', 'c']", &ctx(), &outputs());
        assert_eq!(result, json!(["a", "b", "c"]));
    }

    #[test]
    fn eval_array_literal_empty() {
        let result = evaluate("[]", &ctx(), &outputs());
        assert_eq!(result, json!([]));
    }

    #[test]
    fn eval_array_literal_mixed() {
        let result = evaluate("[1, 'two', true, null]", &ctx(), &outputs());
        assert_eq!(result, json!([1.0, "two", true, null]));
    }

    // --- input path ---

    #[test]
    fn eval_input_path() {
        let ctx = ExecutionContext {
            data: json!({"input": {"question": "Will X?", "price": 0.65}}),
            config: json!({}),
            ..Default::default()
        };
        assert_eq!(
            evaluate("input.question", &ctx, &json!({})),
            json!("Will X?")
        );
        assert_eq!(evaluate("input.price > 0.5", &ctx, &json!({})), json!(true));
    }

    // --- instance_id ---

    #[test]
    fn eval_instance_id() {
        let ctx = ExecutionContext {
            data: json!({}),
            config: json!({}),
            runtime: orch8_types::context::RuntimeContext {
                instance_id: Some("inst-42".to_string()),
                ..Default::default()
            },
            ..Default::default()
        };
        assert_eq!(evaluate("instance_id", &ctx, &json!({})), json!("inst-42"));
    }

    // --- combined expressions ---

    #[test]
    fn eval_combined_in_and_ternary() {
        let ctx = ExecutionContext {
            data: json!({"level": "strong"}),
            config: json!({}),
            ..Default::default()
        };
        assert_eq!(
            evaluate(
                "level in ['strong', 'moderate'] ? 'bet' : 'skip'",
                &ctx,
                &json!({})
            ),
            json!("bet")
        );
    }

    #[test]
    fn eval_abs_with_steps() {
        let outputs = json!({"calc": {"estimate": 0.7}, "price": {"value": 0.5}});
        let result = evaluate(
            "abs(steps.calc.estimate - steps.price.value) * 100",
            &ctx(),
            &outputs,
        );
        let v = result.as_f64().unwrap();
        assert!((v - 20.0).abs() < 0.001, "expected ~20.0, got {v}");
    }

    // --- now() ---

    #[test]
    fn eval_now_returns_rfc3339() {
        let result = evaluate("now()", &ctx(), &outputs());
        let s = result.as_str().unwrap();
        assert!(
            chrono::DateTime::parse_from_rfc3339(s).is_ok(),
            "invalid rfc3339: {s}"
        );
    }

    // --- uuid() ---

    #[test]
    fn eval_uuid_returns_valid_uuid() {
        let result = evaluate("uuid()", &ctx(), &outputs());
        let s = result.as_str().unwrap();
        assert!(uuid::Uuid::parse_str(s).is_ok(), "invalid uuid: {s}");
    }

    // --- random() ---

    #[test]
    fn eval_random_in_range() {
        let result = evaluate("random(1, 10)", &ctx(), &outputs());
        let n = result.as_i64().unwrap_or(-1);
        assert!((1..10).contains(&n), "expected 1..10, got {n}");
    }

    // --- format_date() ---

    #[test]
    fn eval_format_date() {
        let result = evaluate(
            "format_date('2026-01-15T10:30:00+00:00', '%Y-%m-%d')",
            &ctx(),
            &outputs(),
        );
        assert_eq!(result, json!("2026-01-15"));
    }

    // --- day_of_week() ---

    #[test]
    fn eval_day_of_week() {
        // 2026-01-15 is a Thursday (3 = Thu, 0-indexed from Monday)
        let result = evaluate(
            "day_of_week('2026-01-15T10:00:00+00:00')",
            &ctx(),
            &outputs(),
        );
        assert_eq!(result, json!(3));
    }

    // --- keys() ---

    #[test]
    fn eval_keys() {
        let ctx = ExecutionContext {
            data: json!({"obj": {"a": 1, "b": 2}}),
            config: json!({}),
            ..Default::default()
        };
        let result = evaluate("keys(obj)", &ctx, &json!({}));
        let arr = result.as_array().unwrap();
        assert_eq!(arr.len(), 2);
        assert!(arr.contains(&json!("a")));
        assert!(arr.contains(&json!("b")));
    }

    // --- values() ---

    #[test]
    fn eval_values() {
        let ctx = ExecutionContext {
            data: json!({"obj": {"a": 1, "b": 2}}),
            config: json!({}),
            ..Default::default()
        };
        let result = evaluate("values(obj)", &ctx, &json!({}));
        let arr = result.as_array().unwrap();
        assert_eq!(arr.len(), 2);
    }

    // --- contains() ---

    #[test]
    fn eval_contains_array() {
        let ctx = ExecutionContext {
            data: json!({"tags": ["vip", "new"]}),
            config: json!({}),
            ..Default::default()
        };
        assert_eq!(
            evaluate("contains(tags, 'vip')", &ctx, &json!({})),
            json!(true)
        );
        assert_eq!(
            evaluate("contains(tags, 'old')", &ctx, &json!({})),
            json!(false)
        );
    }

    #[test]
    fn eval_contains_string() {
        let ctx = ExecutionContext {
            data: json!({"msg": "hello world"}),
            config: json!({}),
            ..Default::default()
        };
        assert_eq!(
            evaluate("contains(msg, 'world')", &ctx, &json!({})),
            json!(true)
        );
        assert_eq!(
            evaluate("contains(msg, 'xyz')", &ctx, &json!({})),
            json!(false)
        );
    }

    // --- starts_with() ---

    #[test]
    fn eval_starts_with() {
        let ctx = ExecutionContext {
            data: json!({"url": "https://example.com"}),
            config: json!({}),
            ..Default::default()
        };
        assert_eq!(
            evaluate("starts_with(url, 'https')", &ctx, &json!({})),
            json!(true)
        );
        assert_eq!(
            evaluate("starts_with(url, 'http://')", &ctx, &json!({})),
            json!(false)
        );
    }

    // --- ends_with() ---

    #[test]
    fn eval_ends_with() {
        let ctx = ExecutionContext {
            data: json!({"file": "report.pdf"}),
            config: json!({}),
            ..Default::default()
        };
        assert_eq!(
            evaluate("ends_with(file, '.pdf')", &ctx, &json!({})),
            json!(true)
        );
        assert_eq!(
            evaluate("ends_with(file, '.csv')", &ctx, &json!({})),
            json!(false)
        );
    }

    // --- sum() ---

    #[test]
    fn eval_sum() {
        let ctx = ExecutionContext {
            data: json!({"nums": [1, 2, 3, 4]}),
            config: json!({}),
            ..Default::default()
        };
        assert_eq!(evaluate("sum(nums)", &ctx, &json!({})), json!(10.0));
    }

    #[test]
    fn eval_sum_empty() {
        let ctx = ExecutionContext {
            data: json!({"nums": []}),
            config: json!({}),
            ..Default::default()
        };
        assert_eq!(evaluate("sum(nums)", &ctx, &json!({})), json!(0.0));
    }

    // --- avg() ---

    #[test]
    fn eval_avg() {
        let ctx = ExecutionContext {
            data: json!({"nums": [10, 20, 30]}),
            config: json!({}),
            ..Default::default()
        };
        assert_eq!(evaluate("avg(nums)", &ctx, &json!({})), json!(20.0));
    }

    #[test]
    fn eval_avg_empty() {
        let ctx = ExecutionContext {
            data: json!({"nums": []}),
            config: json!({}),
            ..Default::default()
        };
        assert_eq!(evaluate("avg(nums)", &ctx, &json!({})), json!(null));
    }

    // --- min() / max() ---

    #[test]
    fn eval_min_max() {
        let ctx = ExecutionContext {
            data: json!({"nums": [5, 2, 8, 1, 9]}),
            config: json!({}),
            ..Default::default()
        };
        assert_eq!(evaluate("min(nums)", &ctx, &json!({})), json!(1.0));
        assert_eq!(evaluate("max(nums)", &ctx, &json!({})), json!(9.0));
    }

    // --- first() / last() ---

    #[test]
    fn eval_first_last() {
        let ctx = ExecutionContext {
            data: json!({"items": ["a", "b", "c"]}),
            config: json!({}),
            ..Default::default()
        };
        assert_eq!(evaluate("first(items)", &ctx, &json!({})), json!("a"));
        assert_eq!(evaluate("last(items)", &ctx, &json!({})), json!("c"));
    }

    #[test]
    fn eval_first_empty() {
        let ctx = ExecutionContext {
            data: json!({"items": []}),
            config: json!({}),
            ..Default::default()
        };
        assert_eq!(evaluate("first(items)", &ctx, &json!({})), json!(null));
    }

    // --- slice() ---

    #[test]
    fn eval_slice() {
        let ctx = ExecutionContext {
            data: json!({"items": [10, 20, 30, 40, 50]}),
            config: json!({}),
            ..Default::default()
        };
        assert_eq!(
            evaluate("slice(items, 1, 3)", &ctx, &json!({})),
            json!([20, 30])
        );
    }

    #[test]
    fn eval_slice_no_end() {
        let ctx = ExecutionContext {
            data: json!({"items": [10, 20, 30]}),
            config: json!({}),
            ..Default::default()
        };
        assert_eq!(
            evaluate("slice(items, 1)", &ctx, &json!({})),
            json!([20, 30])
        );
    }

    // --- sort() ---

    #[test]
    fn eval_sort_asc() {
        let ctx = ExecutionContext {
            data: json!({"nums": [3, 1, 2]}),
            config: json!({}),
            ..Default::default()
        };
        assert_eq!(evaluate("sort(nums)", &ctx, &json!({})), json!([1, 2, 3]));
    }

    #[test]
    fn eval_sort_desc() {
        let ctx = ExecutionContext {
            data: json!({"nums": [3, 1, 2]}),
            config: json!({}),
            ..Default::default()
        };
        assert_eq!(
            evaluate("sort(nums, 'desc')", &ctx, &json!({})),
            json!([3, 2, 1])
        );
    }

    // --- unique() ---

    #[test]
    fn eval_unique() {
        let ctx = ExecutionContext {
            data: json!({"items": ["a", "b", "a", "c", "b"]}),
            config: json!({}),
            ..Default::default()
        };
        assert_eq!(
            evaluate("unique(items)", &ctx, &json!({})),
            json!(["a", "b", "c"])
        );
    }

    // --- count() ---

    #[test]
    fn eval_count() {
        let ctx = ExecutionContext {
            data: json!({"items": ["a", "b", "a", "c", "a"]}),
            config: json!({}),
            ..Default::default()
        };
        assert_eq!(evaluate("count(items, 'a')", &ctx, &json!({})), json!(3));
    }

    // --- change_pct() ---

    #[test]
    fn eval_change_pct() {
        assert_eq!(
            evaluate("change_pct(100, 150)", &ctx(), &outputs()),
            json!(50.0)
        );
        assert_eq!(
            evaluate("change_pct(200, 100)", &ctx(), &outputs()),
            json!(-50.0)
        );
    }

    #[test]
    fn eval_change_pct_zero_old() {
        assert_eq!(
            evaluate("change_pct(0, 100)", &ctx(), &outputs()),
            json!(null)
        );
    }

    // --- clamp() ---

    #[test]
    fn eval_clamp() {
        assert_eq!(evaluate("clamp(5, 0, 10)", &ctx(), &outputs()), json!(5.0));
        assert_eq!(evaluate("clamp(-5, 0, 10)", &ctx(), &outputs()), json!(0.0));
        assert_eq!(
            evaluate("clamp(15, 0, 10)", &ctx(), &outputs()),
            json!(10.0)
        );
    }

    // --- root variable shortcuts in expressions ---

    #[test]
    fn eval_config_shortcut() {
        assert_eq!(evaluate("config.limit", &ctx(), &outputs()), json!(100));
    }

    #[test]
    fn eval_data_shortcut() {
        assert_eq!(
            evaluate("data.user.name", &ctx(), &outputs()),
            json!("Alice")
        );
    }

    #[test]
    fn eval_runtime_shortcut() {
        let ctx = ExecutionContext {
            data: json!({}),
            config: json!({}),
            runtime: orch8_types::context::RuntimeContext {
                attempt: 3,
                ..Default::default()
            },
            ..Default::default()
        };
        assert_eq!(evaluate("runtime.attempt", &ctx, &json!({})), json!(3));
    }

    // -----------------------------------------------------------------------
    // Warning-capture test infrastructure
    // -----------------------------------------------------------------------

    use std::collections::HashMap;
    use std::sync::{Arc, Mutex};

    #[derive(Debug, Clone)]
    struct CapturedWarning {
        message: String,
        fields: HashMap<String, String>,
    }

    struct WarningCapture {
        warnings: Arc<Mutex<Vec<CapturedWarning>>>,
    }

    impl<S: tracing::Subscriber> tracing_subscriber::Layer<S> for WarningCapture {
        fn on_event(
            &self,
            event: &tracing::Event<'_>,
            _ctx: tracing_subscriber::layer::Context<'_, S>,
        ) {
            if *event.metadata().level() == tracing::Level::WARN {
                let mut collector = FieldCollector::default();
                event.record(&mut collector);
                self.warnings.lock().unwrap().push(CapturedWarning {
                    message: collector.fields.remove("message").unwrap_or_default(),
                    fields: collector.fields,
                });
            }
        }
    }

    #[derive(Default)]
    struct FieldCollector {
        fields: HashMap<String, String>,
    }

    impl tracing::field::Visit for FieldCollector {
        fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
            self.fields
                .insert(field.name().to_string(), format!("{value:?}"));
        }
        fn record_str(&mut self, field: &tracing::field::Field, value: &str) {
            self.fields
                .insert(field.name().to_string(), value.to_string());
        }
    }

    fn capture_warnings<F: FnOnce()>(f: F) -> Vec<CapturedWarning> {
        use tracing_subscriber::layer::SubscriberExt;
        let warnings = Arc::new(Mutex::new(Vec::new()));
        let layer = WarningCapture {
            warnings: Arc::clone(&warnings),
        };
        let subscriber = tracing_subscriber::registry().with(layer);
        tracing::subscriber::with_default(subscriber, f);
        let all = Arc::try_unwrap(warnings).unwrap().into_inner().unwrap();
        all.into_iter()
            .filter(|w| w.message.contains("expression arithmetic"))
            .collect()
    }

    fn eval_expr(expr: &str, c: &ExecutionContext, o: &serde_json::Value) -> serde_json::Value {
        evaluate(expr, c, o)
    }

    // -----------------------------------------------------------------------
    // Null / non-numeric operands → null return value
    // -----------------------------------------------------------------------

    #[test]
    fn add_null_left_returns_null() {
        let c = ctx();
        let o = json!({"s": {"val": null, "num": 5}});
        assert_eq!(eval_expr("steps.s.val + steps.s.num", &c, &o), json!(null));
    }

    #[test]
    fn add_null_right_returns_null() {
        let c = ctx();
        let o = json!({"s": {"num": 5, "val": null}});
        assert_eq!(eval_expr("steps.s.num + steps.s.val", &c, &o), json!(null));
    }

    #[test]
    fn add_both_null_returns_null() {
        let c = ctx();
        let o = json!({"s": {"a": null, "b": null}});
        assert_eq!(eval_expr("steps.s.a + steps.s.b", &c, &o), json!(null));
    }

    #[test]
    fn sub_null_operand_returns_null() {
        let c = ctx();
        let o = json!({"s": {"a": null, "b": 10}});
        assert_eq!(eval_expr("steps.s.a - steps.s.b", &c, &o), json!(null));
    }

    #[test]
    fn sub_object_operand_returns_null() {
        let c = ctx();
        let o = json!({"s": {"obj": {"price": 0.5}, "b": 10}});
        assert_eq!(eval_expr("steps.s.obj - steps.s.b", &c, &o), json!(null));
    }

    #[test]
    fn mul_null_operand_returns_null() {
        let c = ctx();
        let o = json!({"s": {"a": null, "b": 100}});
        assert_eq!(eval_expr("steps.s.a * steps.s.b", &c, &o), json!(null));
    }

    #[test]
    fn mul_array_operand_returns_null() {
        let c = ctx();
        let o = json!({"s": {"arr": [1,2,3], "b": 2}});
        assert_eq!(eval_expr("steps.s.arr * steps.s.b", &c, &o), json!(null));
    }

    #[test]
    fn div_null_operand_returns_null() {
        let c = ctx();
        let o = json!({"s": {"a": 10, "b": null}});
        assert_eq!(eval_expr("steps.s.a / steps.s.b", &c, &o), json!(null));
    }

    #[test]
    fn div_by_zero_returns_null() {
        let c = ctx();
        let o = json!({"s": {"a": 10, "b": 0}});
        assert_eq!(eval_expr("steps.s.a / steps.s.b", &c, &o), json!(null));
    }

    #[test]
    fn div_both_null_returns_null() {
        let c = ctx();
        let o = json!({"s": {"a": null, "b": null}});
        assert_eq!(eval_expr("steps.s.a / steps.s.b", &c, &o), json!(null));
    }

    #[test]
    fn negate_null_returns_null() {
        let c = ctx();
        let o = json!({"s": {"val": null}});
        assert_eq!(eval_expr("-steps.s.val", &c, &o), json!(null));
    }

    #[test]
    fn negate_object_returns_null() {
        let c = ctx();
        let o = json!({"s": {"obj": {"x": 1}}});
        assert_eq!(eval_expr("-steps.s.obj", &c, &o), json!(null));
    }

    #[test]
    fn negate_array_returns_null() {
        let c = ctx();
        let o = json!({"s": {"arr": [1, 2]}});
        assert_eq!(eval_expr("-steps.s.arr", &c, &o), json!(null));
    }

    #[test]
    fn negate_non_numeric_string_returns_null() {
        let c = ctx();
        let o = json!({"s": {"val": "hello"}});
        assert_eq!(eval_expr("-steps.s.val", &c, &o), json!(null));
    }

    #[test]
    fn add_non_numeric_string_returns_null() {
        let c = ctx();
        let o = json!({"s": {"a": "hello", "b": 5}});
        assert_eq!(eval_expr("steps.s.a + steps.s.b", &c, &o), json!(null));
    }

    #[test]
    fn sub_non_numeric_string_returns_null() {
        let c = ctx();
        let o = json!({"s": {"a": 10, "b": "world"}});
        assert_eq!(eval_expr("steps.s.a - steps.s.b", &c, &o), json!(null));
    }

    #[test]
    fn mul_non_numeric_string_returns_null() {
        let c = ctx();
        let o = json!({"s": {"a": "abc", "b": "def"}});
        assert_eq!(eval_expr("steps.s.a * steps.s.b", &c, &o), json!(null));
    }

    #[test]
    fn div_non_numeric_string_returns_null() {
        let c = ctx();
        let o = json!({"s": {"a": "xyz", "b": 2}});
        assert_eq!(eval_expr("steps.s.a / steps.s.b", &c, &o), json!(null));
    }

    // --- missing paths resolve to null in arithmetic ---

    #[test]
    fn add_missing_path_returns_null() {
        let c = ctx();
        let o = json!({"s": {"a": 5}});
        assert_eq!(
            eval_expr("steps.s.a + steps.s.missing", &c, &o),
            json!(null)
        );
    }

    #[test]
    fn sub_missing_path_returns_null() {
        let c = ctx();
        let o = json!({"s": {"a": 5}});
        assert_eq!(
            eval_expr("steps.s.a - steps.s.missing", &c, &o),
            json!(null)
        );
    }

    // --- valid arithmetic still works ---

    #[test]
    fn add_two_numbers() {
        let c = ctx();
        let o = json!({"s": {"a": 3, "b": 7}});
        assert_eq!(eval_expr("steps.s.a + steps.s.b", &c, &o), json!(10.0));
    }

    #[test]
    fn sub_two_numbers() {
        let c = ctx();
        let o = json!({"s": {"a": 10, "b": 3}});
        assert_eq!(eval_expr("steps.s.a - steps.s.b", &c, &o), json!(7.0));
    }

    #[test]
    fn mul_two_numbers() {
        let c = ctx();
        let o = json!({"s": {"a": 4, "b": 5}});
        assert_eq!(eval_expr("steps.s.a * steps.s.b", &c, &o), json!(20.0));
    }

    #[test]
    fn div_two_numbers() {
        let c = ctx();
        let o = json!({"s": {"a": 20, "b": 4}});
        assert_eq!(eval_expr("steps.s.a / steps.s.b", &c, &o), json!(5.0));
    }

    #[test]
    fn negate_number_works() {
        let c = ctx();
        let o = json!({"s": {"val": 42}});
        assert_eq!(eval_expr("-steps.s.val", &c, &o), json!(-42.0));
    }

    #[test]
    fn add_string_coercion_numbers() {
        let c = ctx();
        let o = json!({"s": {"a": "3.5", "b": 2}});
        assert_eq!(eval_expr("steps.s.a + steps.s.b", &c, &o), json!(5.5));
    }

    #[test]
    fn sub_string_coercion_numbers() {
        let c = ctx();
        let o = json!({"s": {"a": "10", "b": "3"}});
        assert_eq!(eval_expr("steps.s.a - steps.s.b", &c, &o), json!(7.0));
    }

    #[test]
    fn negate_numeric_string_works() {
        let c = ctx();
        let o = json!({"s": {"val": "7.5"}});
        assert_eq!(eval_expr("-steps.s.val", &c, &o), json!(-7.5));
    }

    #[test]
    fn bool_true_coerces_to_one_in_arithmetic() {
        let c = ctx();
        let o = json!({"s": {"a": true, "b": 5}});
        assert_eq!(eval_expr("steps.s.a + steps.s.b", &c, &o), json!(6.0));
    }

    #[test]
    fn bool_false_coerces_to_zero_in_arithmetic() {
        let c = ctx();
        let o = json!({"s": {"a": false, "b": 10}});
        assert_eq!(eval_expr("steps.s.a + steps.s.b", &c, &o), json!(10.0));
    }

    // --- complex expressions ---

    #[test]
    fn complex_expression_with_null_intermediate() {
        let c = ctx();
        let o = json!({"s": {"a": null, "b": 10}});
        assert_eq!(
            eval_expr("(steps.s.a - steps.s.b) * 100", &c, &o),
            json!(null)
        );
    }

    #[test]
    fn complex_expression_with_valid_numbers() {
        let c = ctx();
        let o = json!({"s": {"est": 0.65, "price": 0.5}});
        let result = eval_expr("(steps.s.est - steps.s.price) * 100", &c, &o);
        let val = result.as_f64().unwrap();
        assert!((val - 15.0).abs() < 0.001);
    }

    // -----------------------------------------------------------------------
    // Warning emission: verify message AND structured fields
    // -----------------------------------------------------------------------

    #[test]
    fn warn_on_null_addition_with_fields() {
        let c = ctx();
        let o = json!({"s": {"a": null, "b": 5}});
        let warnings = capture_warnings(|| {
            eval_expr("steps.s.a + steps.s.b", &c, &o);
        });
        assert_eq!(
            warnings.len(),
            1,
            "expected exactly 1 warning, got: {warnings:?}"
        );
        let w = &warnings[0];
        assert!(w.message.contains("non-numeric operand in addition"));
        assert_eq!(w.fields.get("left").map(String::as_str), Some("null"));
        assert_eq!(w.fields.get("right").map(String::as_str), Some("5"));
    }

    #[test]
    fn warn_on_null_subtraction_with_fields() {
        let c = ctx();
        let o = json!({"s": {"a": null, "b": 10}});
        let warnings = capture_warnings(|| {
            let result = eval_expr("steps.s.a - steps.s.b", &c, &o);
            assert_eq!(result, json!(null));
        });
        assert_eq!(warnings.len(), 1);
        let w = &warnings[0];
        assert!(w.message.contains("non-numeric operand in subtraction"));
        assert_eq!(w.fields.get("left").map(String::as_str), Some("null"));
        assert_eq!(w.fields.get("right").map(String::as_str), Some("10"));
    }

    #[test]
    fn warn_on_null_multiplication_with_fields() {
        let c = ctx();
        let o = json!({"s": {"a": null, "b": 5}});
        let warnings = capture_warnings(|| {
            eval_expr("steps.s.a * steps.s.b", &c, &o);
        });
        assert_eq!(warnings.len(), 1);
        assert!(warnings[0]
            .message
            .contains("non-numeric operand in multiplication"));
        assert_eq!(
            warnings[0].fields.get("left").map(String::as_str),
            Some("null")
        );
    }

    #[test]
    fn warn_on_division_by_zero_with_fields() {
        let c = ctx();
        let o = json!({"s": {"a": 10, "b": 0}});
        let warnings = capture_warnings(|| {
            let result = eval_expr("steps.s.a / steps.s.b", &c, &o);
            assert_eq!(result, json!(null));
        });
        assert_eq!(warnings.len(), 1);
        assert!(warnings[0].message.contains("division by zero"));
        assert_eq!(
            warnings[0].fields.get("left").map(String::as_str),
            Some("10")
        );
        assert_eq!(
            warnings[0].fields.get("right").map(String::as_str),
            Some("0")
        );
    }

    #[test]
    fn warn_on_null_division_with_fields() {
        let c = ctx();
        let o = json!({"s": {"a": 10, "b": null}});
        let warnings = capture_warnings(|| {
            eval_expr("steps.s.a / steps.s.b", &c, &o);
        });
        assert_eq!(warnings.len(), 1);
        assert!(warnings[0]
            .message
            .contains("non-numeric operand in division"));
        assert_eq!(
            warnings[0].fields.get("right").map(String::as_str),
            Some("null")
        );
    }

    #[test]
    fn warn_on_object_subtraction_includes_serialized_object() {
        let c = ctx();
        let o = json!({"s": {"obj": {"price": 0.5}, "b": 10}});
        let warnings = capture_warnings(|| {
            eval_expr("steps.s.obj - steps.s.b", &c, &o);
        });
        assert_eq!(warnings.len(), 1);
        assert!(warnings[0]
            .message
            .contains("non-numeric operand in subtraction"));
        let left = warnings[0].fields.get("left").unwrap();
        assert!(
            left.contains("price"),
            "left field should contain the object, got: {left}"
        );
    }

    #[test]
    fn warn_on_array_multiplication_includes_serialized_array() {
        let c = ctx();
        let o = json!({"s": {"arr": [1,2,3], "b": 2}});
        let warnings = capture_warnings(|| {
            eval_expr("steps.s.arr * steps.s.b", &c, &o);
        });
        assert_eq!(warnings.len(), 1);
        assert!(warnings[0]
            .message
            .contains("non-numeric operand in multiplication"));
        let left = warnings[0].fields.get("left").unwrap();
        assert!(
            left.contains('['),
            "left field should contain the array, got: {left}"
        );
    }

    #[test]
    fn warn_on_negation_of_null_with_field() {
        let c = ctx();
        let o = json!({"s": {"val": null}});
        let warnings = capture_warnings(|| {
            eval_expr("-steps.s.val", &c, &o);
        });
        assert_eq!(warnings.len(), 1);
        assert!(warnings[0]
            .message
            .contains("non-numeric operand in negation"));
        assert_eq!(
            warnings[0].fields.get("value").map(String::as_str),
            Some("null")
        );
    }

    #[test]
    fn warn_on_negation_of_object_with_field() {
        let c = ctx();
        let o = json!({"s": {"obj": {"x": 1}}});
        let warnings = capture_warnings(|| {
            eval_expr("-steps.s.obj", &c, &o);
        });
        assert_eq!(warnings.len(), 1);
        assert!(warnings[0]
            .message
            .contains("non-numeric operand in negation"));
        let val = warnings[0].fields.get("value").unwrap();
        assert!(
            val.contains('x'),
            "value field should contain the object, got: {val}"
        );
    }

    #[test]
    fn warn_on_non_numeric_string_addition() {
        let c = ctx();
        let o = json!({"s": {"a": "hello", "b": 5}});
        let warnings = capture_warnings(|| {
            eval_expr("steps.s.a + steps.s.b", &c, &o);
        });
        assert_eq!(warnings.len(), 1);
        assert!(warnings[0]
            .message
            .contains("non-numeric operand in addition"));
        assert!(warnings[0].fields.get("left").unwrap().contains("hello"));
    }

    // --- no false positives ---

    #[test]
    fn no_warn_on_valid_arithmetic() {
        let c = ctx();
        let o = json!({"s": {"a": 10, "b": 3}});
        let warnings = capture_warnings(|| {
            let result = eval_expr("steps.s.a - steps.s.b", &c, &o);
            assert_eq!(result, json!(7.0));
        });
        assert!(
            warnings.is_empty(),
            "no warnings expected for valid arithmetic, got: {warnings:?}"
        );
    }

    #[test]
    fn no_warn_on_string_coercion_arithmetic() {
        let c = ctx();
        let o = json!({"s": {"a": "3.5", "b": 2}});
        let warnings = capture_warnings(|| {
            eval_expr("steps.s.a + steps.s.b", &c, &o);
        });
        assert!(
            warnings.is_empty(),
            "numeric string coercion should not warn, got: {warnings:?}"
        );
    }

    #[test]
    fn no_warn_on_bool_arithmetic() {
        let c = ctx();
        let o = json!({"s": {"a": true, "b": 5}});
        let warnings = capture_warnings(|| {
            eval_expr("steps.s.a + steps.s.b", &c, &o);
        });
        assert!(
            warnings.is_empty(),
            "bool coercion should not warn, got: {warnings:?}"
        );
    }

    #[test]
    fn no_warn_on_negation_of_number() {
        let c = ctx();
        let o = json!({"s": {"val": 42}});
        let warnings = capture_warnings(|| {
            eval_expr("-steps.s.val", &c, &o);
        });
        assert!(
            warnings.is_empty(),
            "negation of number should not warn, got: {warnings:?}"
        );
    }

    // --- cascade: chained null produces exactly the expected number of warnings ---

    #[test]
    fn cascade_null_sub_then_mul_produces_two_warnings() {
        let c = ctx();
        let o = json!({"s": {"a": null, "b": 10}});
        let warnings = capture_warnings(|| {
            let result = eval_expr("(steps.s.a - steps.s.b) * 100", &c, &o);
            assert_eq!(result, json!(null));
        });
        assert_eq!(
            warnings.len(),
            2,
            "expected 2 warnings (sub then mul), got {}: {warnings:?}",
            warnings.len()
        );
        assert!(warnings[0].message.contains("subtraction"));
        assert!(warnings[1].message.contains("multiplication"));
    }

    #[test]
    fn cascade_null_add_then_div_produces_two_warnings() {
        let c = ctx();
        let o = json!({"s": {"a": null, "b": 5}});
        let warnings = capture_warnings(|| {
            let result = eval_expr("(steps.s.a + steps.s.b) / 10", &c, &o);
            assert_eq!(result, json!(null));
        });
        assert_eq!(warnings.len(), 2);
        assert!(warnings[0].message.contains("addition"));
        assert!(warnings[1].message.contains("division"));
    }

    #[test]
    fn triple_cascade_null_across_three_ops() {
        let c = ctx();
        let o = json!({"s": {"a": null, "b": 10}});
        let warnings = capture_warnings(|| {
            let result = eval_expr("(steps.s.a - steps.s.b) * 100 + 1", &c, &o);
            assert_eq!(result, json!(null));
        });
        assert_eq!(
            warnings.len(),
            3,
            "expected 3 warnings (sub, mul, add), got {}: {warnings:?}",
            warnings.len()
        );
    }
}
