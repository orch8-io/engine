//! A bounded subset of Amazon States Language `JSONPath` intrinsics.
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::io::{self, Write};

const TEMPLATE_PREFIX: &str = "{{@asl ";
const CALLBACK_TEMPLATE_PREFIX: &str = "{{@asl_callback ";
/// The only intrinsic reference resolved from a worker claim.
pub const CALLBACK_TOKEN_PATH: &str = "$$.Task.Token";
const MAX_DEPTH: usize = 10;
const MAX_ARGUMENTS: usize = 128;
const MAX_RESULT_BYTES: usize = 256 * 1024;
const MAX_EVALUATION_BYTES: usize = 16 * MAX_RESULT_BYTES;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind", content = "value", rename_all = "snake_case")]
pub enum Intrinsic {
    String(String),
    Literal(Value),
    Path(String),
    Array(Vec<Self>),
    Format(Vec<Self>),
    StringToJson(Box<Self>),
    PutResult {
        input: Box<Self>,
        result: Box<Self>,
        path: Vec<String>,
    },
}

#[derive(Debug, PartialEq, Eq, thiserror::Error)]
#[error("{0}")]
pub struct IntrinsicError(&'static str);

const TOO_LARGE: IntrinsicError = IntrinsicError("ASL intrinsic exceeds its size or nesting limit");
const BAD_FORMAT: IntrinsicError =
    IntrinsicError("invalid ASL intrinsic string escapes or format placeholders");

/// Translate one intrinsic, resolving reference paths in the imported state graph.
pub fn parse(
    source: &str,
    mut resolve: impl FnMut(&str) -> Option<String>,
) -> Result<Intrinsic, IntrinsicError> {
    if source.len() > MAX_RESULT_BYTES {
        return Err(TOO_LARGE);
    }
    let mut parser = Parser {
        input: source,
        resolve: &mut resolve,
    };
    let call = parser.read(0)?;
    if !parser.input.trim().is_empty()
        || !matches!(
            call,
            Intrinsic::Array(_) | Intrinsic::Format(_) | Intrinsic::StringToJson(_)
        )
    {
        return Err(IntrinsicError("expected one complete ASL intrinsic call"));
    }
    Ok(call)
}

struct Parser<'a, F> {
    input: &'a str,
    resolve: &'a mut F,
}

impl<F: FnMut(&str) -> Option<String>> Parser<'_, F> {
    fn take(&mut self, symbol: char) -> bool {
        self.input = self.input.trim_start();
        if let Some(rest) = self.input.strip_prefix(symbol) {
            self.input = rest;
            true
        } else {
            false
        }
    }

    fn read(&mut self, depth: usize) -> Result<Intrinsic, IntrinsicError> {
        self.input = self.input.trim_start();
        if self.take('\'') {
            let mut escape = false;
            for (offset, character) in self.input.char_indices() {
                if escape {
                    escape = false;
                } else if character == '\\' {
                    escape = true;
                } else if character == '\'' {
                    let content = self.input[..offset].to_string();
                    self.input = &self.input[offset + 1..];
                    return Ok(Intrinsic::String(content));
                }
            }
            return Err(IntrinsicError("unterminated ASL string argument"));
        }
        if let Some(rest) = self.input.strip_prefix("States.") {
            if depth >= MAX_DEPTH {
                return Err(TOO_LARGE);
            }
            let length = rest.bytes().take_while(u8::is_ascii_alphabetic).count();
            let (name, tail) = rest.split_at(length);
            self.input = tail;
            if !self.take('(') {
                return Err(IntrinsicError("expected opening intrinsic parenthesis"));
            }
            let mut arguments = Vec::new();
            if !self.take(')') {
                loop {
                    if arguments.len() == MAX_ARGUMENTS {
                        return Err(TOO_LARGE);
                    }
                    arguments.push(self.read(depth + 1)?);
                    if self.take(')') {
                        break;
                    }
                    if !self.take(',') {
                        return Err(IntrinsicError("expected a comma or closing parenthesis"));
                    }
                }
            }
            return match name {
                "Array" => Ok(Intrinsic::Array(arguments)),
                "Format" if !arguments.is_empty() => Ok(Intrinsic::Format(arguments)),
                "StringToJson" if arguments.len() == 1 => {
                    Ok(Intrinsic::StringToJson(Box::new(arguments.remove(0))))
                }
                "Format" | "StringToJson" => {
                    Err(IntrinsicError("incorrect ASL intrinsic argument count"))
                }
                _ => Err(IntrinsicError("unsupported ASL intrinsic")),
            };
        }
        let length = self
            .input
            .find(|character: char| character.is_whitespace() || matches!(character, ',' | ')'))
            .unwrap_or(self.input.len());
        let token = &self.input[..length];
        self.input = &self.input[length..];
        if token.starts_with('$') {
            return (self.resolve)(token)
                .map(Intrinsic::Path)
                .ok_or(IntrinsicError("ASL intrinsic reference cannot be resolved"));
        }
        let value: Value = serde_json::from_str(token)
            .map_err(|_| IntrinsicError("invalid ASL literal argument"))?;
        if !matches!(value, Value::Null | Value::Bool(_) | Value::Number(_)) {
            return Err(IntrinsicError("invalid ASL literal argument"));
        }
        Ok(Intrinsic::Literal(value))
    }
}

impl Intrinsic {
    /// Encode a translated intrinsic as a standalone typed workflow template.
    pub fn to_template(&self) -> Result<String, IntrinsicError> {
        self.encode_template(TEMPLATE_PREFIX)
    }

    /// Encode an intrinsic whose callback token is supplied only at delivery.
    pub fn to_callback_template(&self) -> Result<String, IntrinsicError> {
        self.encode_template(CALLBACK_TEMPLATE_PREFIX)
    }

    fn encode_template(&self, prefix: &str) -> Result<String, IntrinsicError> {
        let data = serde_json::to_string(self)
            .map_err(|_| IntrinsicError("invalid ASL intrinsic template"))?;
        if data.len() > MAX_RESULT_BYTES {
            return Err(TOO_LARGE);
        }
        Ok(format!("{prefix}{data}}}}}"))
    }

    /// Recognize a standalone intrinsic template emitted by the importer.
    pub fn from_template(template: &str) -> Option<Result<Self, IntrinsicError>> {
        Self::decode_template(template, TEMPLATE_PREFIX)
    }

    pub fn from_callback_template(template: &str) -> Option<Result<Self, IntrinsicError>> {
        Self::decode_template(template, CALLBACK_TEMPLATE_PREFIX)
    }

    fn decode_template(template: &str, prefix: &str) -> Option<Result<Self, IntrinsicError>> {
        let data = template.strip_prefix(prefix)?;
        let result = (|| {
            let data = data
                .strip_suffix("}}")
                .ok_or(IntrinsicError("unclosed ASL intrinsic template"))?;
            if data.len() > MAX_RESULT_BYTES {
                return Err(TOO_LARGE);
            }
            serde_json::from_str(data).map_err(|_| IntrinsicError("invalid ASL intrinsic template"))
        })();
        Some(result)
    }

    /// Freeze workflow references before enqueueing. Only the token stays unbound.
    pub fn bind_callback(
        mut self,
        mut read_path: impl FnMut(&str) -> Option<Value>,
    ) -> Result<Self, IntrinsicError> {
        let mut budget = MAX_EVALUATION_BYTES;
        self.bind_callback_inner(&mut read_path, 0, &mut budget)?;
        self.evaluate_callback("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")?;
        Ok(self)
    }

    fn bind_callback_inner(
        &mut self,
        read_path: &mut impl FnMut(&str) -> Option<Value>,
        depth: usize,
        budget: &mut usize,
    ) -> Result<(), IntrinsicError> {
        if depth > MAX_DEPTH {
            return Err(TOO_LARGE);
        }
        match self {
            Self::Path(path) if path != CALLBACK_TOKEN_PATH => {
                let value =
                    read_path(path).ok_or(IntrinsicError("ASL intrinsic reference is missing"))?;
                serde_json::to_writer(ByteBudget(budget), &value).map_err(|_| TOO_LARGE)?;
                *self = Self::Literal(value);
            }
            Self::Array(args) | Self::Format(args) => {
                if args.len() > MAX_ARGUMENTS {
                    return Err(TOO_LARGE);
                }
                for arg in args {
                    arg.bind_callback_inner(read_path, depth + 1, budget)?;
                }
            }
            Self::StringToJson(arg) => arg.bind_callback_inner(read_path, depth + 1, budget)?,
            Self::PutResult { input, result, .. } => {
                input.bind_callback_inner(read_path, depth + 1, budget)?;
                result.bind_callback_inner(read_path, depth + 1, budget)?;
            }
            Self::Literal(_) | Self::String(_) | Self::Path(_) => {}
        }
        Ok(())
    }

    /// Materialize the already-bound expression for a worker's delivery copy.
    pub fn evaluate_callback(&self, token: &str) -> Result<Value, IntrinsicError> {
        self.evaluate(|path| (path == CALLBACK_TOKEN_PATH).then(|| Value::String(token.to_owned())))
    }

    /// Evaluate one translated expression and preserve the native JSON result type.
    pub fn evaluate(
        &self,
        mut read_path: impl FnMut(&str) -> Option<Value>,
    ) -> Result<Value, IntrinsicError> {
        let mut budget = MAX_EVALUATION_BYTES;
        self.evaluate_inner(&mut read_path, 0, &mut budget)
    }

    fn evaluate_inner(
        &self,
        read_path: &mut impl FnMut(&str) -> Option<Value>,
        depth: usize,
        budget: &mut usize,
    ) -> Result<Value, IntrinsicError> {
        if depth > MAX_DEPTH {
            return Err(TOO_LARGE);
        }
        let value = match self {
            Self::Literal(value) => value.clone(),
            Self::String(text) => Value::String(expand(text, &[], false)?),
            Self::Path(path) => {
                read_path(path).ok_or(IntrinsicError("ASL intrinsic reference is missing"))?
            }
            Self::StringToJson(input) => {
                let input = input.evaluate_inner(read_path, depth + 1, budget)?;
                serde_json::from_str(
                    input
                        .as_str()
                        .ok_or(IntrinsicError("States.StringToJson requires a string"))?,
                )
                .map_err(|_| IntrinsicError("States.StringToJson received invalid JSON"))?
            }
            Self::Array(values) => {
                if values.len() > MAX_ARGUMENTS {
                    return Err(TOO_LARGE);
                }
                Value::Array(
                    values
                        .iter()
                        .map(|value| value.evaluate_inner(read_path, depth + 1, budget))
                        .collect::<Result<Vec<_>, _>>()?,
                )
            }
            Self::Format(values) => {
                if values.len() > MAX_ARGUMENTS {
                    return Err(TOO_LARGE);
                }
                let (pattern, arguments) = values.split_first().ok_or(BAD_FORMAT)?;
                let pattern = match pattern {
                    Self::String(raw) => raw.clone(),
                    other => match other.evaluate_inner(read_path, depth + 1, budget)? {
                        Value::String(pattern) => pattern,
                        _ => return Err(IntrinsicError("States.Format requires a string pattern")),
                    },
                };
                let formatted = arguments
                    .iter()
                    .map(|argument| argument.evaluate_inner(read_path, depth + 1, budget))
                    .collect::<Result<Vec<_>, _>>()?;
                Value::String(expand(&pattern, &formatted, true)?)
            }
            Self::PutResult {
                input,
                result,
                path,
            } => {
                let mut input = input.evaluate_inner(read_path, depth + 1, budget)?;
                let result = result.evaluate_inner(read_path, depth + 1, budget)?;
                put_result(&mut input, path, result)?;
                input
            }
        };
        let mut result_bytes = MAX_RESULT_BYTES;
        serde_json::to_writer(ByteBudget(&mut result_bytes), &value).map_err(|_| TOO_LARGE)?;
        *budget = budget
            .checked_sub(MAX_RESULT_BYTES - result_bytes)
            .ok_or(TOO_LARGE)?;
        Ok(value)
    }
}

fn put_result(input: &mut Value, path: &[String], result: Value) -> Result<(), IntrinsicError> {
    if path.len() > MAX_ARGUMENTS || path.iter().any(String::is_empty) {
        return Err(TOO_LARGE);
    }
    let mut destination = input;
    for key in path {
        destination = match destination {
            Value::Object(object) => object
                .entry(key)
                .or_insert_with(|| Value::Object(serde_json::Map::new())),
            Value::Array(items) => key
                .parse::<usize>()
                .ok()
                .and_then(|index| items.get_mut(index))
                .ok_or(IntrinsicError(
                    "ASL ResultPath does not address an array element",
                ))?,
            _ => {
                return Err(IntrinsicError(
                    "ASL ResultPath traverses a non-container value",
                ));
            }
        };
    }
    *destination = result;
    Ok(())
}

struct ByteBudget<'a>(&'a mut usize);

impl Write for ByteBudget<'_> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        *self.0 = self
            .0
            .checked_sub(buf.len())
            .ok_or_else(|| io::Error::other("ASL payload limit"))?;
        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

fn expand(pattern: &str, arguments: &[Value], formatting: bool) -> Result<String, IntrinsicError> {
    let mut output = String::new();
    let mut pattern = pattern.chars();
    let mut args = arguments.iter();
    while let Some(character) = pattern.next() {
        match character {
            '\\' => {
                let escaped = pattern.next().ok_or(BAD_FORMAT)?;
                if !matches!(escaped, '\\' | '\'' | '{' | '}') {
                    return Err(BAD_FORMAT);
                }
                output.push(escaped);
            }
            '{' if formatting => {
                if pattern.next() != Some('}') {
                    return Err(BAD_FORMAT);
                }
                match args.next().ok_or(BAD_FORMAT)? {
                    Value::String(text) => output.push_str(text),
                    other => output.push_str(&other.to_string()),
                }
            }
            '{' | '}' => return Err(BAD_FORMAT),
            other => output.push(other),
        }
        if output.len() > MAX_RESULT_BYTES {
            return Err(TOO_LARGE);
        }
    }
    if args.next().is_some() {
        return Err(BAD_FORMAT);
    }
    Ok(output)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn callback_template_freezes_business_inputs_and_can_be_evaluated_with_fresh_tokens() {
        let source = r#"States.Array(States.Format('{}:{}', $.label, $$.Task.Token), States.StringToJson(States.Format('\{"token":"{}"\}', $$.Task.Token)))"#;
        let expression = parse(source, |path| Some(path.to_owned())).unwrap();
        let template = expression.to_callback_template().unwrap();
        assert!(Intrinsic::from_template(&template).is_none());
        let expression = Intrinsic::from_callback_template(&template)
            .unwrap()
            .unwrap();
        assert!(expression.clone().bind_callback(|_| None).is_err());
        let bound = expression
            .bind_callback(|path| {
                assert_eq!(path, "$.label");
                Some(json!("job"))
            })
            .unwrap();
        let retained = serde_json::to_string(&bound).unwrap();
        let restored: Intrinsic = serde_json::from_str(&retained).unwrap();
        for token in ["first", "fresh"] {
            assert!(!retained.contains(token));
            assert_eq!(
                restored.evaluate_callback(token).unwrap(),
                json!([format!("job:{token}"), {"token":token}])
            );
        }
    }

    #[test]
    fn callback_binding_rejects_an_unquoted_token_as_json() {
        let expression = parse("States.StringToJson($$.Task.Token)", |path| {
            Some(path.to_owned())
        })
        .unwrap();
        assert!(expression.bind_callback(|_| None).is_err());
    }

    #[test]
    fn puts_projected_result_into_original_document() {
        let original = json!({"records":[{}, {"untouched":true}], "other":7});
        let projected = json!({"nested":[null, {"code":42}]});
        let expression = Intrinsic::PutResult {
            input: Box::new(Intrinsic::Path("data".into())),
            result: Box::new(Intrinsic::Path("outputs.projection".into())),
            path: ["records", "1", "extra", "result"]
                .map(str::to_owned)
                .into(),
        };
        let evaluate = |expression: &Intrinsic| {
            expression.evaluate(|path| match path {
                "data" => Some(original.clone()),
                "outputs.projection" => Some(projected.clone()),
                _ => None,
            })
        };
        let template = expression.to_template().unwrap();
        assert_eq!(
            evaluate(&Intrinsic::from_template(&template).unwrap().unwrap()).unwrap(),
            json!({
                "records":[{}, {"untouched":true, "extra":{"result":projected}}], "other":7
            })
        );
        let mut non_container = original;
        assert!(
            put_result(
                &mut non_container,
                &["other".into(), "result".into()],
                Value::Null
            )
            .is_err()
        );
    }

    #[test]
    fn nested_format_array_and_json_keep_typed_values_and_escapes() {
        let input = r"States.Array(States.Format('\{\} {} \'{}\' \\', $.name, 7), States.StringToJson($.doc), true, null)";
        let ast = parse(input, |path| Some(path.to_owned())).unwrap();
        assert_eq!(
            ast.evaluate(|path| match path {
                "$.name" => Some(json!("green")),
                "$.doc" => Some(json!(r#"{"active":true}"#)),
                _ => None,
            })
            .unwrap(),
            json!(["{} green '7' \\", {"active":true}, true, null])
        );
    }

    #[test]
    fn invalid_syntax_arity_nesting_paths_and_native_types_are_rejected() {
        for invalid in [
            "States.Unsupported(1)",
            "States.Array(true,)",
            "States.StringToJson()",
            "States.Array('ab\\')",
            "States.Array(1) outside",
            "States.Array($.missing)",
        ] {
            assert!(parse(invalid, |_| None).is_err(), "{invalid}");
        }
        let too_deep = "States.Array(".repeat(11) + &")".repeat(11);
        assert_eq!(parse(&too_deep, |_| None).unwrap_err(), TOO_LARGE);
        for expression in [
            "States.Format('{}')",
            "States.Format('{}',1,2)",
            "States.Format('\\q')",
            "States.Format(10)",
            "States.StringToJson($.value)",
            "States.Array($.missing)",
        ] {
            let ast = parse(expression, |path| Some(path.to_owned())).unwrap();
            assert!(
                ast.evaluate(|path| (path == "$.value").then_some(json!(42)))
                    .is_err(),
                "{expression}"
            );
        }
    }
}
