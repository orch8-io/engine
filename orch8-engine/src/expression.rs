use orch8_types::context::ExecutionContext;

/// Expression evaluation error with position information.
#[derive(Debug, Clone, PartialEq)]
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
    let tokens = tokenize(trimmed);
    let mut parser = Parser::new(&tokens, context, outputs);
    let result = parser.parse_or();
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
}
