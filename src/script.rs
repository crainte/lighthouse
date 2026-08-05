//! Custom UP/DOWN evaluation in Lua.
//!
//! This runs code supplied by whoever has admin access, inside the coordinator
//! process, on every result. Three things therefore matter more than features:
//!
//! * **No ambient authority.** The interpreter is built with only the pure
//!   libraries (string/table/math). No `io`, no `os`, no `package`, no
//!   `require`, no `debug` — so a script cannot read files, spawn processes, or
//!   load native code.
//! * **It must terminate.** An instruction-count hook aborts runaway scripts, so
//!   `while true do end` costs a bounded slice of one ingest instead of wedging
//!   the coordinator forever.
//! * **It must not panic the caller.** Every failure — syntax, runtime, timeout,
//!   wrong return type — comes back as `Err(String)` for the caller to log or
//!   show in the editor.
//!
//! A fresh interpreter per evaluation keeps scripts from leaking state into each
//! other. That costs microseconds, which is irrelevant next to a network probe.

use mlua::{Lua, StdLib, Value};
use std::collections::HashMap;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;

/// Ceiling on how much work one script may do. Generous for real eval logic
/// (a few branches over a body string), instant death for an infinite loop.
const INSTRUCTION_LIMIT: u32 = 200_000;

/// Bodies are attacker-influenced and only used transiently, but a script that
/// scans one shouldn't be handed an unbounded string. Matches the design doc.
pub const MAX_BODY_BYTES: usize = 64 * 1024;

/// What a probe observed, as the script sees it.
#[derive(Debug, Default, Clone)]
pub struct EvalInput {
    /// HTTP status, or 0 when the probe never got a response.
    pub status: u16,
    pub latency_ms: u64,
    pub body: Option<String>,
    pub headers: HashMap<String, String>,
}

/// A script's verdict. `latency_ms` is the script's optional override.
#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub struct EvalOutcome {
    pub up: bool,
    pub latency_ms: Option<u64>,
}

/// Compile a script without running it, so a syntax error is caught when it is
/// saved rather than silently marking a service down every interval afterwards.
pub fn check_syntax(source: &str) -> Result<(), String> {
    let lua = sandbox()?;
    lua.load(wrap(source))
        .into_function()
        .map(|_| ())
        .map_err(|e| format!("Lua syntax error: {e}"))
}

/// Run `source` against one observation.
pub fn evaluate(source: &str, input: &EvalInput) -> Result<EvalOutcome, String> {
    let lua = sandbox()?;

    let globals = lua.globals();
    globals
        .set("status", input.status)
        .map_err(|e| e.to_string())?;
    globals
        .set("latency", input.latency_ms)
        .map_err(|e| e.to_string())?;
    // Absent body is an empty string rather than nil: `#body` and `string.find`
    // are the obvious things to reach for and both would error on nil.
    globals
        .set("body", input.body.clone().unwrap_or_default())
        .map_err(|e| e.to_string())?;
    let headers = lua.create_table().map_err(|e| e.to_string())?;
    for (k, v) in &input.headers {
        headers
            .set(k.as_str(), v.as_str())
            .map_err(|e| e.to_string())?;
    }
    globals.set("headers", headers).map_err(|e| e.to_string())?;

    let chunk = lua.load(wrap(source));
    let returned: mlua::MultiValue = chunk
        .eval()
        .map_err(|e| format!("Lua error: {}", trim_lua_error(&e.to_string())))?;

    interpret(returned)
}

/// Build an interpreter with no way to touch the host.
fn sandbox() -> Result<Lua, String> {
    // StdLib::NONE plus the pure libraries: everything reachable from here is
    // computation over values we passed in.
    let lua = Lua::new_with(
        StdLib::STRING | StdLib::TABLE | StdLib::MATH,
        mlua::LuaOptions::default(),
    )
    .map_err(|e| format!("could not create Lua sandbox: {e}"))?;

    // Belt and braces: even with those libraries excluded, make sure the usual
    // escape hatches are absent rather than merely unpopulated.
    {
        let globals = lua.globals();
        for name in [
            "io",
            "os",
            "package",
            "require",
            "dofile",
            "loadfile",
            "load",
            "loadstring",
            "debug",
            "collectgarbage",
            "rawset",
            "rawget",
            "setmetatable",
            "getmetatable",
        ] {
            let _ = globals.set(name, Value::Nil);
        }
    }

    // The hook is `Fn`, so the counter needs interior mutability. It lives as
    // long as the interpreter, which is one evaluation.
    let executed = Arc::new(AtomicU32::new(0));
    lua.set_hook(
        mlua::HookTriggers::new().every_nth_instruction(1000),
        move |_lua, _debug| {
            if executed.fetch_add(1000, Ordering::Relaxed) + 1000 > INSTRUCTION_LIMIT {
                // Surfaces to the caller as a normal Lua error, so a runaway
                // script is reported like any other bad script.
                return Err(mlua::Error::RuntimeError(
                    "script exceeded its instruction budget (infinite loop?)".into(),
                ));
            }
            Ok(())
        },
    );
    Ok(lua)
}

/// Wrap the snippet in a function body so `return` works at the top level, which
/// is how the design document writes these scripts.
fn wrap(source: &str) -> String {
    format!("return (function()\n{source}\nend)()")
}

/// Lua prefixes errors with a chunk name and line offset that reflect our
/// wrapper, not the user's script. Strip it so the editor shows something the
/// author can act on.
fn trim_lua_error(msg: &str) -> String {
    msg.lines()
        .next()
        .unwrap_or(msg)
        .rsplit_once("]:")
        .map(|(_, rest)| rest.trim_start_matches(|c: char| c.is_ascii_digit() || c == ':'))
        .unwrap_or(msg)
        .trim()
        .to_string()
}

fn interpret(values: mlua::MultiValue) -> Result<EvalOutcome, String> {
    let mut iter = values.into_iter();
    let verdict = iter.next().unwrap_or(Value::Nil);
    let up = match &verdict {
        Value::String(s) => {
            let s = s.to_str().map_err(|e| e.to_string())?.trim().to_lowercase();
            match s.as_str() {
                "up" => true,
                "down" => false,
                other => {
                    return Err(format!(
                        "script returned {other:?}; expected \"UP\" or \"DOWN\""
                    ))
                }
            }
        }
        // A bare boolean is the obvious thing to write by mistake, and its intent
        // is unambiguous, so accept it rather than failing the check.
        Value::Boolean(b) => *b,
        Value::Nil => {
            return Err("script returned nothing; expected \"UP\" or \"DOWN\"".to_string())
        }
        other => {
            return Err(format!(
                "script returned a {}; expected \"UP\" or \"DOWN\"",
                other.type_name()
            ))
        }
    };

    let latency_ms = match iter.next() {
        Some(Value::Integer(i)) if i >= 0 => Some(i as u64),
        Some(Value::Number(n)) if n >= 0.0 => Some(n as u64),
        _ => None,
    };
    Ok(EvalOutcome { up, latency_ms })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn input(status: u16) -> EvalInput {
        EvalInput {
            status,
            latency_ms: 42,
            ..Default::default()
        }
    }

    /// The design document's own example, which is the contract users will copy.
    const DESIGN_DOC_EXAMPLE: &str = r#"
        local code = math.floor(status / 100)
        if status == 429 or (code >= 2 and code <= 3) then
          return "UP", latency
        end
        return "DOWN", latency
    "#;

    #[test]
    fn the_documented_example_classifies_as_documented() {
        for (status, expected_up) in [
            (200, true),
            (301, true),
            (429, true),
            (500, false),
            (0, false),
        ] {
            let out = evaluate(DESIGN_DOC_EXAMPLE, &input(status)).unwrap();
            assert_eq!(out.up, expected_up, "status {status}");
            assert_eq!(out.latency_ms, Some(42), "latency passes through");
        }
    }

    #[test]
    fn a_script_can_read_body_and_headers() {
        let mut i = input(200);
        i.body = Some("status: degraded".into());
        i.headers.insert("content-type".into(), "text/plain".into());

        let by_body = evaluate(
            r#"if string.find(body, "degraded") then return "DOWN" end return "UP""#,
            &i,
        )
        .unwrap();
        assert!(!by_body.up, "a 200 with a bad body is still DOWN");

        let by_header = evaluate(
            r#"if headers["content-type"] == "text/plain" then return "UP" end return "DOWN""#,
            &i,
        )
        .unwrap();
        assert!(by_header.up);
    }

    #[test]
    fn a_script_may_override_the_recorded_latency() {
        let out = evaluate(r#"return "UP", 7"#, &input(200)).unwrap();
        assert_eq!(out.latency_ms, Some(7));
        // No second value means "keep what the probe measured".
        let out = evaluate(r#"return "UP""#, &input(200)).unwrap();
        assert_eq!(out.latency_ms, None);
    }

    #[test]
    fn a_missing_body_is_an_empty_string_not_nil() {
        // `#body` on nil would be a runtime error, which would mark the service
        // down for a reason that has nothing to do with the service.
        let out = evaluate(
            r#"if #body == 0 then return "UP" end return "DOWN""#,
            &input(200),
        );
        assert!(out.unwrap().up);
    }

    #[test]
    fn syntax_errors_are_caught_before_a_script_is_ever_used() {
        let err = check_syntax("this is not lua").unwrap_err();
        assert!(err.contains("syntax"), "got {err}");
        assert!(check_syntax(DESIGN_DOC_EXAMPLE).is_ok());
    }

    #[test]
    fn a_script_that_returns_nonsense_is_an_error_not_a_verdict() {
        // Silently treating these as UP would hide an outage; as DOWN it would
        // page someone at 3am. Neither is acceptable, so it must be an error.
        for src in [
            r#"return "maybe""#,
            r#"return 200"#,
            r#"return nil"#,
            r#"local x = 1"#,
            r#"return {}"#,
        ] {
            assert!(
                evaluate(src, &input(200)).is_err(),
                "{src} should be rejected"
            );
        }
        // A plain boolean is unambiguous enough to honour.
        assert!(evaluate("return true", &input(200)).unwrap().up);
        assert!(!evaluate("return false", &input(200)).unwrap().up);
    }

    #[test]
    fn a_runtime_error_is_reported_rather_than_panicking() {
        let err = evaluate(r#"return nil + 1"#, &input(200)).unwrap_err();
        assert!(err.to_lowercase().contains("lua"), "got {err}");
    }

    #[test]
    fn an_infinite_loop_terminates_instead_of_wedging_the_coordinator() {
        let start = std::time::Instant::now();
        let err = evaluate("while true do end", &input(200)).unwrap_err();
        assert!(
            err.contains("instruction budget"),
            "expected the budget to stop it, got {err}"
        );
        assert!(
            start.elapsed() < std::time::Duration::from_secs(5),
            "took {:?}, the hook is not firing",
            start.elapsed()
        );
    }

    #[test]
    fn scripts_cannot_reach_the_host() {
        // Each of these is a way out of the sandbox: filesystem, process
        // execution, native library loading, and loading further code.
        for src in [
            r#"return io.open("/etc/passwd") and "UP" or "DOWN""#,
            r#"return os.execute("id") and "UP" or "DOWN""#,
            r#"return os.getenv("WARDEN_ADMIN_PASSWORD") and "UP" or "DOWN""#,
            r#"return require("os") and "UP" or "DOWN""#,
            r#"return load("return 1") and "UP" or "DOWN""#,
            r#"return dofile("/etc/passwd") and "UP" or "DOWN""#,
            r#"return debug.getinfo(1) and "UP" or "DOWN""#,
        ] {
            let result = evaluate(src, &input(200));
            assert!(
                result.is_err(),
                "{src} must not succeed — it reached the host"
            );
        }
    }

    #[test]
    fn one_script_cannot_leave_state_for_the_next() {
        // A fresh interpreter per evaluation; a global set by one script must be
        // invisible to the next, or scripts could interfere with each other.
        evaluate(r#"leaked = "yes" return "UP""#, &input(200)).unwrap();
        let out = evaluate(
            r#"if leaked == nil then return "UP" end return "DOWN""#,
            &input(200),
        );
        assert!(out.unwrap().up, "state leaked between evaluations");
    }
}
