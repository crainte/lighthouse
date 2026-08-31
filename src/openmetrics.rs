//! Prometheus/OpenMetrics text exposition parsing and sample selection.
//!
//! The agent scrapes an exporter, parses the text exposition format here, and
//! selects one sample by metric family name plus exact label matching. Only the
//! selected numeric value and the same-scrape samples travel to the coordinator
//! for evaluation; the raw body is never persisted.
//!
//! This is deliberately not PromQL. Histograms and summaries are exposed as the
//! individual samples the exporter emits (`_bucket`, `_sum`, `_count`, quantile
//! series) with their labels; no rate, increase, or quantile math happens here.

use serde::{Deserialize, Serialize};

/// One numeric sample from a scrape: a metric family name, its label set, and a
/// value.
///
/// Labels are carried as sorted `(name, value)` pairs so equality and the Lua
/// serialization are deterministic regardless of the order the exporter wrote
/// them. Construct through [`MetricSample::new`] to preserve that invariant.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MetricSample {
    /// The metric family name, e.g. `http_requests_total`.
    pub name: String,
    /// Sorted `(label, value)` pairs. Empty for an unlabeled sample.
    pub labels: Vec<(String, String)>,
    /// The sample's numeric value. NaN and infinities are representable; the
    /// selection and evaluation layers decide policy on them.
    pub value: f64,
}

impl MetricSample {
    /// Build a sample, sorting its labels so equality and serialization are
    /// order-independent.
    pub fn new(name: impl Into<String>, mut labels: Vec<(String, String)>, value: f64) -> Self {
        labels.sort();
        Self {
            name: name.into(),
            labels,
            value,
        }
    }

    /// Whether this sample's labels contain every `(name, value)` in `wanted`
    /// with exactly those values. Extra labels on the sample are allowed unless
    /// the caller requires an exact set (see [`MetricSample::labels_equal`]).
    pub fn matches_labels(&self, wanted: &[(String, String)]) -> bool {
        wanted
            .iter()
            .all(|(k, v)| self.labels.iter().any(|(lk, lv)| lk == k && lv == v))
    }

    /// Whether this sample's label set is exactly `wanted` (same names, same
    /// values, no extras). This is the selection semantics: a metric name plus
    /// an exact label set identifies at most one sample.
    pub fn labels_equal(&self, wanted: &[(String, String)]) -> bool {
        self.labels.len() == wanted.len() && self.matches_labels(wanted)
    }
}

/// Why parsing or selecting a sample failed. Kept separate from the probe's
/// `FailureKind` so the parser has no dependency on the result model; the agent
/// maps these onto the existing failure categories.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MetricError {
    /// The body was not valid exposition text (malformed sample, bad label
    /// syntax, non-numeric value where a number was required).
    Parse(String),
    /// No sample matched the configured name and labels.
    NotFound(String),
    /// More than one sample matched the configured name and labels, so the
    /// selection is ambiguous and must not silently pick one.
    Ambiguous(String),
}

impl std::fmt::Display for MetricError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            MetricError::Parse(m) => write!(f, "{m}"),
            MetricError::NotFound(m) => write!(f, "{m}"),
            MetricError::Ambiguous(m) => write!(f, "{m}"),
        }
    }
}

impl std::error::Error for MetricError {}

/// Parse a Prometheus/OpenMetrics text exposition body into its samples.
///
/// Handles: `# HELP`/`# TYPE` metadata and other comment lines, blank lines,
/// unlabeled samples, `name{a="1",b="2"}` label sets with escaped values
/// (`\\`, `\"`, `\n`), scientific and integer values, the OpenMetrics `# EOF`
/// terminator, and the special float tokens `Nan`, `+Inf`, `-Inf`. An optional
/// timestamp column is accepted and ignored.
///
/// Rejects malformed samples, malformed label syntax, and non-numeric values
/// as [`MetricError::Parse`]. Duplicate identical (name+labels) samples are a
/// parse error rather than a silently-kept collision.
pub fn parse(body: &str) -> Result<Vec<MetricSample>, MetricError> {
    let mut samples: Vec<MetricSample> = Vec::new();
    // Track exact (name, sorted-labels) identities already seen so a repeated
    // series is a parse error rather than a silently kept collision that would
    // make selection ambiguous for no visible reason.
    let mut seen: std::collections::HashSet<(String, Vec<(String, String)>)> =
        std::collections::HashSet::new();

    for raw in body.lines() {
        let line = raw.trim();
        if line.is_empty() {
            continue;
        }
        // The OpenMetrics document terminator. Anything after it is not part of
        // the exposition, so stop rather than trying to parse it.
        if line == "# EOF" {
            break;
        }
        // HELP/TYPE/UNIT metadata and any other comment. We keep no metadata:
        // selection is by name and labels, and types do not change how a raw
        // sample is read.
        if line.starts_with('#') {
            continue;
        }

        let sample = parse_sample(line)?;
        let identity = (sample.name.clone(), sample.labels.clone());
        if !seen.insert(identity) {
            return Err(MetricError::Parse(format!(
                "duplicate sample for {:?} with identical labels",
                sample.name
            )));
        }
        samples.push(sample);
    }

    Ok(samples)
}

/// Parse one non-comment, non-blank exposition line into a sample.
///
/// Grammar: `name[{label="value",...}] value [timestamp]`. The optional
/// timestamp column is accepted and ignored — this feature reads instantaneous
/// values only, so a scrape timestamp changes nothing here.
fn parse_sample(line: &str) -> Result<MetricSample, MetricError> {
    let bytes = line.as_bytes();
    let mut i = 0;

    // Metric name: [a-zA-Z_:][a-zA-Z0-9_:]*
    let name_start = i;
    if i >= bytes.len() || !is_name_start(bytes[i]) {
        return Err(MetricError::Parse(format!(
            "invalid metric name in {line:?}"
        )));
    }
    i += 1;
    while i < bytes.len() && is_name_char(bytes[i]) {
        i += 1;
    }
    let name = &line[name_start..i];

    // Optional label set.
    let labels = if i < bytes.len() && bytes[i] == b'{' {
        let (parsed, next) = parse_labels(line, i)?;
        i = next;
        parsed
    } else {
        Vec::new()
    };

    // At least one whitespace must separate the metric from its value.
    let before = i;
    while i < bytes.len() && (bytes[i] == b' ' || bytes[i] == b'\t') {
        i += 1;
    }
    if i == before {
        return Err(MetricError::Parse(format!(
            "expected a value after the metric in {line:?}"
        )));
    }

    // Value runs to the next whitespace; an optional timestamp may follow and is
    // ignored.
    let value_start = i;
    while i < bytes.len() && bytes[i] != b' ' && bytes[i] != b'\t' {
        i += 1;
    }
    let value_str = &line[value_start..i];
    if value_str.is_empty() {
        return Err(MetricError::Parse(format!("missing value in {line:?}")));
    }
    let value = parse_value(value_str).ok_or_else(|| {
        MetricError::Parse(format!("non-numeric value {value_str:?} in {line:?}"))
    })?;

    // Anything remaining is at most a timestamp column (ignored). A second
    // non-whitespace token beyond that is malformed.
    let rest = line[i..].trim();
    if !rest.is_empty() {
        // The timestamp, if present, is a single numeric token. Accept exactly
        // one and reject trailing garbage so a malformed line is not read as a
        // valid sample.
        let mut it = rest.split_whitespace();
        let ts = it.next().unwrap_or_default();
        if ts.parse::<f64>().is_err() || it.next().is_some() {
            return Err(MetricError::Parse(format!(
                "unexpected trailing content {rest:?} in {line:?}"
            )));
        }
    }

    Ok(MetricSample::new(name, labels, value))
}

/// Parse a `{a="1",b="2"}` label set starting at `open` (the `{`). Returns the
/// labels and the index just past the closing `}`.
fn parse_labels(line: &str, open: usize) -> Result<(Vec<(String, String)>, usize), MetricError> {
    let bytes = line.as_bytes();
    let mut i = open + 1; // past '{'
    let mut labels = Vec::new();

    loop {
        // Skip whitespace between entries.
        while i < bytes.len() && (bytes[i] == b' ' || bytes[i] == b'\t') {
            i += 1;
        }
        if i < bytes.len() && bytes[i] == b'}' {
            return Ok((labels, i + 1));
        }
        if i >= bytes.len() {
            return Err(MetricError::Parse(format!(
                "unterminated label set in {line:?}"
            )));
        }

        // Label name: [a-zA-Z_][a-zA-Z0-9_]*
        let ln_start = i;
        if !is_label_name_start(bytes[i]) {
            return Err(MetricError::Parse(format!(
                "invalid label name in {line:?}"
            )));
        }
        i += 1;
        while i < bytes.len() && is_label_name_char(bytes[i]) {
            i += 1;
        }
        let lname = line[ln_start..i].to_string();

        while i < bytes.len() && (bytes[i] == b' ' || bytes[i] == b'\t') {
            i += 1;
        }
        if i >= bytes.len() || bytes[i] != b'=' {
            return Err(MetricError::Parse(format!(
                "expected '=' after label name in {line:?}"
            )));
        }
        i += 1;
        while i < bytes.len() && (bytes[i] == b' ' || bytes[i] == b'\t') {
            i += 1;
        }
        if i >= bytes.len() || bytes[i] != b'"' {
            return Err(MetricError::Parse(format!(
                "label value must be quoted in {line:?}"
            )));
        }
        i += 1; // past opening quote

        // Quoted value with escapes.
        let mut value = String::new();
        loop {
            if i >= bytes.len() {
                return Err(MetricError::Parse(format!(
                    "unterminated label value in {line:?}"
                )));
            }
            match bytes[i] {
                b'"' => {
                    i += 1;
                    break;
                }
                b'\\' => {
                    i += 1;
                    if i >= bytes.len() {
                        return Err(MetricError::Parse(format!(
                            "dangling escape in label value in {line:?}"
                        )));
                    }
                    match bytes[i] {
                        b'\\' => value.push('\\'),
                        b'"' => value.push('"'),
                        b'n' => value.push('\n'),
                        other => {
                            // Unknown escape: keep it literal rather than
                            // guessing, so exotic exporters do not fail a scrape.
                            value.push('\\');
                            value.push(other as char);
                        }
                    }
                    i += 1;
                }
                _ => {
                    // Advance over one UTF-8 char to keep multibyte values intact.
                    let ch = line[i..].chars().next().unwrap();
                    value.push(ch);
                    i += ch.len_utf8();
                }
            }
        }
        labels.push((lname, value));

        // Separator or end.
        while i < bytes.len() && (bytes[i] == b' ' || bytes[i] == b'\t') {
            i += 1;
        }
        match bytes.get(i) {
            Some(b',') => {
                i += 1;
            }
            Some(b'}') => {
                return Ok((labels, i + 1));
            }
            _ => {
                return Err(MetricError::Parse(format!(
                    "expected ',' or '}}' in label set in {line:?}"
                )))
            }
        }
    }
}

/// Parse a Prometheus float, including the special tokens `Nan`, `+Inf`, `-Inf`.
fn parse_value(s: &str) -> Option<f64> {
    match s {
        _ if s.eq_ignore_ascii_case("nan") => Some(f64::NAN),
        _ if s.eq_ignore_ascii_case("+inf") || s.eq_ignore_ascii_case("inf") => Some(f64::INFINITY),
        _ if s.eq_ignore_ascii_case("-inf") => Some(f64::NEG_INFINITY),
        // Rust's f64 parser accepts the decimal and scientific forms Prometheus
        // uses; it also accepts its own "inf"/"nan" spellings, already handled
        // above, so there is no divergent second path.
        _ => s.parse::<f64>().ok(),
    }
}

fn is_name_start(b: u8) -> bool {
    b.is_ascii_alphabetic() || b == b'_' || b == b':'
}
fn is_name_char(b: u8) -> bool {
    b.is_ascii_alphanumeric() || b == b'_' || b == b':'
}
fn is_label_name_start(b: u8) -> bool {
    b.is_ascii_alphabetic() || b == b'_'
}
fn is_label_name_char(b: u8) -> bool {
    b.is_ascii_alphanumeric() || b == b'_'
}

/// Select the single sample identified by `name` and the exact `labels` set
/// from an already-parsed scrape.
///
/// Missing selection is [`MetricError::NotFound`]; more than one match is
/// [`MetricError::Ambiguous`]. Neither is treated as a zero — an unmatched
/// selection is a failed observation, per the design.
pub fn select<'a>(
    samples: &'a [MetricSample],
    name: &str,
    labels: &[(String, String)],
) -> Result<&'a MetricSample, MetricError> {
    let mut hits = samples
        .iter()
        .filter(|s| s.name == name && s.labels_equal(labels));
    match (hits.next(), hits.next()) {
        (None, _) => Err(MetricError::NotFound(format!(
            "no sample for metric {name:?} with the configured labels"
        ))),
        (Some(one), None) => Ok(one),
        (Some(_), Some(_)) => Err(MetricError::Ambiguous(format!(
            "metric {name:?} matched more than one sample with the configured labels"
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn pairs(kv: &[(&str, &str)]) -> Vec<(String, String)> {
        kv.iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    }

    #[test]
    fn metadata_and_blanks_are_ignored() {
        let body = "\
# HELP http_requests_total The total number of HTTP requests.
# TYPE http_requests_total counter

http_requests_total 1027
# a stray comment
";
        let s = parse(body).unwrap();
        assert_eq!(s.len(), 1);
        assert_eq!(s[0].name, "http_requests_total");
        assert!(s[0].labels.is_empty());
        assert_eq!(s[0].value, 1027.0);
    }

    #[test]
    fn labels_are_order_independent_after_parse() {
        let a = parse("m{b=\"2\",a=\"1\"} 5").unwrap();
        let b = parse("m{a=\"1\",b=\"2\"} 5").unwrap();
        assert_eq!(a, b);
        assert_eq!(a[0].labels, pairs(&[("a", "1"), ("b", "2")]));
    }

    #[test]
    fn escaped_label_values_are_unescaped() {
        let s = parse(r#"m{path="a\\b\"c\nd"} 1"#).unwrap();
        assert_eq!(s[0].labels, pairs(&[("path", "a\\b\"c\nd")]));
    }

    #[test]
    fn scientific_and_special_values() {
        assert_eq!(parse("m 1.5e3").unwrap()[0].value, 1500.0);
        assert_eq!(parse("m -2.5").unwrap()[0].value, -2.5);
        assert!(parse("m Nan").unwrap()[0].value.is_nan());
        assert_eq!(parse("m +Inf").unwrap()[0].value, f64::INFINITY);
        assert_eq!(parse("m -Inf").unwrap()[0].value, f64::NEG_INFINITY);
    }

    #[test]
    fn timestamp_column_is_ignored() {
        let s = parse("m 7 1595853000000").unwrap();
        assert_eq!(s[0].value, 7.0);
    }

    #[test]
    fn openmetrics_eof_terminates() {
        let s = parse("m 1\n# EOF\nignored_after 999").unwrap();
        assert_eq!(s.len(), 1);
        assert_eq!(s[0].name, "m");
    }

    #[test]
    fn histogram_and_summary_series_are_plain_samples() {
        let body = "\
lat_bucket{le=\"0.5\"} 3
lat_bucket{le=\"+Inf\"} 5
lat_sum 4.2
lat_count 5
q{quantile=\"0.99\"} 0.7
";
        let s = parse(body).unwrap();
        assert_eq!(s.len(), 5);
        assert_eq!(
            select(&s, "lat_bucket", &pairs(&[("le", "0.5")]))
                .unwrap()
                .value,
            3.0
        );
    }

    #[test]
    fn malformed_inputs_are_rejected() {
        assert!(matches!(
            parse("m{bad label} 1"),
            Err(MetricError::Parse(_))
        ));
        assert!(matches!(parse("m notanumber"), Err(MetricError::Parse(_))));
        assert!(matches!(
            parse("m{a=\"unterminated} 1"),
            Err(MetricError::Parse(_))
        ));
        assert!(matches!(parse("m{a=1} 1"), Err(MetricError::Parse(_))));
        assert!(matches!(parse("m"), Err(MetricError::Parse(_))));
    }

    #[test]
    fn duplicate_identical_sample_is_rejected() {
        let body = "m{a=\"1\"} 1\nm{a=\"1\"} 2";
        assert!(matches!(parse(body), Err(MetricError::Parse(_))));
    }

    #[test]
    fn select_requires_exact_label_set() {
        let s = parse("m{a=\"1\",b=\"2\"} 9").unwrap();
        // A subset does not select: selection is by the exact label set.
        assert!(matches!(
            select(&s, "m", &pairs(&[("a", "1")])),
            Err(MetricError::NotFound(_))
        ));
        assert_eq!(
            select(&s, "m", &pairs(&[("a", "1"), ("b", "2")]))
                .unwrap()
                .value,
            9.0
        );
    }

    #[test]
    fn select_by_exact_labels_disambiguates_a_family() {
        // Exact-set matching means an empty selector matches only an unlabeled
        // series. Against two labeled series it selects neither: the caller must
        // name the label that distinguishes them, which is the whole point of
        // exact matching.
        let s = parse("up{job=\"a\"} 1\nup{job=\"b\"} 0").unwrap();
        assert!(matches!(
            select(&s, "up", &[]),
            Err(MetricError::NotFound(_))
        ));
        assert!(matches!(
            select(&s, "nope", &[]),
            Err(MetricError::NotFound(_))
        ));
        assert_eq!(
            select(&s, "up", &pairs(&[("job", "a")])).unwrap().value,
            1.0
        );
        assert_eq!(
            select(&s, "up", &pairs(&[("job", "b")])).unwrap().value,
            0.0
        );
    }

    #[test]
    fn select_is_ambiguous_only_when_two_samples_share_a_name_and_labels() {
        // The parser rejects exact duplicates, so genuine ambiguity requires
        // constructing samples directly. Confirm select flags it rather than
        // silently returning the first.
        let s = vec![
            MetricSample::new("m", pairs(&[("a", "1")]), 1.0),
            MetricSample::new("m", pairs(&[("a", "1")]), 2.0),
        ];
        assert!(matches!(
            select(&s, "m", &pairs(&[("a", "1")])),
            Err(MetricError::Ambiguous(_))
        ));
    }
}
