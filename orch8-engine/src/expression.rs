use orch8_types::context::ExecutionContext;

/// Evaluate a simple expression against context and outputs.
///
/// Supported syntax:
/// - Path references: `context.data.x`, `outputs.step_1.result`
/// - Literals: `"hello"`, `42`, `true`, `false`, `null`
/// - Comparisons: `==`, `!=`, `>`, `<`, `>=`, `<=`
/// - Logical: `&&`, `||`
/// - Arithmetic: `+`, `-`, `*`, `/`
/// - Unary negation: `!expr`
///
/// Returns the evaluated result as a JSON value.
pub fn evaluate(
    expr: &str,
    context: &ExecutionContext,
    outputs: &serde_json::Value,
) -> serde_json::Value {
    let tokens = tokenize(expr.trim());
    let mut parser = Parser::new(&tokens, context, outputs);
    parser.parse_or()
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
    fn new(
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

    fn enter(&mut self) -> bool {
        self.depth += 1;
        self.depth <= MAX_PARSE_DEPTH
    }

    fn leave(&mut self) {
        self.depth -= 1;
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

    // comparison: additive (== | != | > | >= | < | <= additive)?
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

    // unary: !unary | primary
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
        let result = self.parse_primary();
        self.leave();
        result
    }

    // primary: literal | path | (expr)
    fn parse_primary(&mut self) -> serde_json::Value {
        match self.advance().cloned() {
            Some(Token::String(s)) => serde_json::Value::String(s),
            Some(Token::Number(n)) => serde_json::json!(n),
            Some(Token::Bool(b)) => serde_json::Value::Bool(b),
            Some(Token::Path(path)) => self.resolve_path(&path),
            Some(Token::LParen) => {
                let val = self.parse_or();
                if self.peek() == Some(&Token::RParen) {
                    self.advance();
                }
                val
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
            Some("outputs") => navigate_json(self.outputs, &parts[1..]),
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
    match (to_f64(a), to_f64(b)) {
        (Some(a), Some(b)) => serde_json::json!(a + b),
        _ => serde_json::Value::Null,
    }
}

fn json_sub(a: &serde_json::Value, b: &serde_json::Value) -> serde_json::Value {
    match (to_f64(a), to_f64(b)) {
        (Some(a), Some(b)) => serde_json::json!(a - b),
        _ => serde_json::Value::Null,
    }
}

fn json_mul(a: &serde_json::Value, b: &serde_json::Value) -> serde_json::Value {
    match (to_f64(a), to_f64(b)) {
        (Some(a), Some(b)) => serde_json::json!(a * b),
        _ => serde_json::Value::Null,
    }
}

fn json_div(a: &serde_json::Value, b: &serde_json::Value) -> serde_json::Value {
    match (to_f64(a), to_f64(b)) {
        (Some(a), Some(b)) if b != 0.0 => serde_json::json!(a / b),
        _ => serde_json::Value::Null,
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
}
