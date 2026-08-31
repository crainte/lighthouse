//! Notification backends for sending alerts.
//!
//! Supports multiple notification methods (webhook, NTFY, etc.) with a common
//! interface. Each notifier is configured in the coordinator config and processes
//! transitions independently.

use crate::coordinator::state::Transition;
use crate::models::Status;
use lettre::message::Mailbox;
use lettre::transport::smtp::authentication::Credentials;
use lettre::{AsyncSmtpTransport, AsyncTransport, Message, Tokio1Executor};
use reqwest::Client;
use serde::{Deserialize, Serialize};

/// A notification backend that can send alerts for check transitions.
///
/// `name` is the routing key: a check names the notifiers it wants, so the
/// label has to be stable and unique. Renaming one is therefore a routing
/// change, and the admin API reports the checks it orphans rather than letting
/// them go quietly unrouted.
#[derive(Debug, Clone, Deserialize, Serialize, PartialEq)]
pub struct Notifier {
    /// Blank only for a notifier written before names existed. It still
    /// receives default-routed alerts; it just cannot be routed to by name
    /// until an operator names it, which the admin page requires on save.
    #[serde(default)]
    pub name: String,
    #[serde(flatten)]
    pub kind: NotifierKind,
    /// Receives alerts from every check that does not name its notifiers.
    ///
    /// Defaults to true when absent, which is what keeps an existing
    /// deployment paging after an upgrade: its notifiers predate routing and
    /// were, in effect, all defaults.
    #[serde(default = "yes")]
    pub default: bool,
}

fn yes() -> bool {
    true
}

/// Blank strings a form submits are absence, not a value. Applied to optional
/// fields so a cleared input does not become an empty username.
fn trimmed(v: Option<String>) -> Option<String> {
    v.map(|s| s.trim().to_string()).filter(|s| !s.is_empty())
}

fn is_http(url: &str) -> bool {
    url.starts_with("http://") || url.starts_with("https://")
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq)]
#[serde(tag = "type", rename_all = "lowercase")]
pub enum NotifierKind {
    /// Webhook: POST transition JSON to a URL.
    Webhook { url: String },
    /// NTFY: Publish transition to an NTFY topic.
    Ntfy {
        topic: String,
        #[serde(default)]
        base_url: Option<String>,
    },
    /// Slack incoming webhook. Deliberately a separate kind from `Webhook`:
    /// Slack does not accept arbitrary JSON, it wants a `text` field, so
    /// pointing the generic webhook at a Slack URL looks configured and
    /// answers 400 `invalid_payload` on every alert.
    Slack { webhook_url: String },
    /// PagerDuty Events API v2. The check id is the dedup key, so the DOWN
    /// triggers an incident and the matching UP resolves *that* incident
    /// rather than opening a second one nobody closes.
    Pagerduty { routing_key: String },
    /// SMTP. A newtype variant so the config has a name of its own — with
    /// `tag = "type"` it still serialises flat, like the others.
    Email(MailConfig),
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq)]
pub struct MailConfig {
    pub host: String,
    /// Absent means the standard port for the TLS mode: 465 implicit, 587
    /// STARTTLS, 25 none.
    #[serde(default)]
    pub port: Option<u16>,
    /// Both or neither — a username with no password cannot authenticate, and
    /// a relay that wanted no credentials would not have been given one.
    #[serde(default)]
    pub username: Option<String>,
    #[serde(default)]
    pub password: Option<String>,
    pub from: String,
    /// One address. ponytail: a distribution list is the mail server's job; an
    /// alias costs nothing and keeps a recipient editor out of this panel.
    pub to: String,
    #[serde(default)]
    pub tls: MailTls,
}

/// How the SMTP connection is encrypted. Explicit rather than guessed from the
/// port, because both wrong guesses are bad: plaintext credentials, or a
/// handshake that can never succeed.
#[derive(Debug, Clone, Copy, Deserialize, Serialize, PartialEq, Eq, Default)]
#[serde(rename_all = "lowercase")]
pub enum MailTls {
    /// Connect in the clear, then upgrade — *required*, never opportunistic, so
    /// a relay that does not offer STARTTLS fails instead of leaking the login.
    #[default]
    Starttls,
    /// TLS from the first byte (SMTPS).
    Implicit,
    /// None. For a trusted relay on localhost, or a private CA the bundled
    /// webpki roots do not know.
    None,
}

impl Notifier {
    /// Trim the strings a form submits. A trailing space in a URL, a name or an
    /// ntfy topic is invisible in the field and breaks delivery or routing.
    pub fn sanitized(self) -> Self {
        Self {
            name: self.name.trim().to_string(),
            kind: match self.kind {
                NotifierKind::Webhook { url } => NotifierKind::Webhook {
                    url: url.trim().to_string(),
                },
                NotifierKind::Ntfy { topic, base_url } => NotifierKind::Ntfy {
                    topic: topic.trim().to_string(),
                    base_url: trimmed(base_url),
                },
                NotifierKind::Slack { webhook_url } => NotifierKind::Slack {
                    webhook_url: webhook_url.trim().to_string(),
                },
                NotifierKind::Pagerduty { routing_key } => NotifierKind::Pagerduty {
                    routing_key: routing_key.trim().to_string(),
                },
                // The password is the one field left untouched: a space can be
                // part of it, and silently trimming one locks the operator out
                // of their own relay.
                NotifierKind::Email(m) => NotifierKind::Email(MailConfig {
                    host: m.host.trim().to_string(),
                    username: trimmed(m.username),
                    password: m.password.filter(|p| !p.is_empty()),
                    from: m.from.trim().to_string(),
                    to: m.to.trim().to_string(),
                    ..m
                }),
            },
            default: self.default,
        }
    }

    /// Rejections are read in the notifier form, so they say what to fix.
    pub fn validate(&self) -> Result<(), String> {
        if self.name.is_empty() {
            return Err("every notifier needs a name — it is what a check routes to".into());
        }
        match &self.kind {
            NotifierKind::Webhook { url } => {
                if url.is_empty() {
                    return Err(format!("{} needs a URL", self.name));
                }
                if !is_http(url) {
                    return Err("a webhook URL has to start with http:// or https://".into());
                }
            }
            NotifierKind::Ntfy { topic, base_url } => {
                if topic.is_empty() {
                    return Err(format!("{} needs an ntfy topic", self.name));
                }
                // A topic is a path segment; a slash in it silently posts to a
                // different topic than the one shown in the form.
                if topic.contains('/') {
                    return Err("an ntfy topic cannot contain a slash".into());
                }
                if let Some(base) = base_url {
                    if !is_http(base) {
                        return Err(
                            "the ntfy server URL has to start with http:// or https://".into()
                        );
                    }
                }
            }
            // Not pinned to hooks.slack.com: Mattermost and Discord's
            // compatibility endpoint take the same `text` payload, and
            // rejecting them would buy nothing.
            NotifierKind::Slack { webhook_url } => {
                if webhook_url.is_empty() {
                    return Err(format!("{} needs a Slack webhook URL", self.name));
                }
                if !is_http(webhook_url) {
                    return Err("a Slack webhook URL has to start with http:// or https://".into());
                }
            }
            NotifierKind::Pagerduty { routing_key } => {
                if routing_key.is_empty() {
                    return Err(format!(
                        "{} needs a PagerDuty Events API v2 routing key",
                        self.name
                    ));
                }
            }
            NotifierKind::Email(m) => {
                if m.host.is_empty() {
                    return Err(format!("{} needs an SMTP host", self.name));
                }
                for (label, addr) in [("From", &m.from), ("To", &m.to)] {
                    if addr.parse::<Mailbox>().is_err() {
                        return Err(format!("{label} is not a usable email address: {addr:?}"));
                    }
                }
                if m.username.is_some() != m.password.is_some() {
                    return Err("SMTP needs a username and a password together, or neither".into());
                }
            }
        }
        Ok(())
    }
}

/// Which notifiers a transition goes to.
///
/// Resolved against the live list at send time rather than at enqueue: the
/// queue holds what the check asked for, not a snapshot of the fleet of
/// backends, so editing a notifier never changes the meaning of a queued alert.
pub fn targets<'a>(all: &'a [Notifier], routed_to: &[String]) -> Vec<&'a Notifier> {
    if routed_to.is_empty() {
        all.iter().filter(|n| n.default).collect()
    } else {
        all.iter()
            .filter(|n| routed_to.iter().any(|r| r == &n.name))
            .collect()
    }
}

const NTFY_DEFAULT: &str = "https://ntfy.sh";

/// Send a transition notification via the given notifier.
///
/// Best-effort at the dispatcher, which logs the error and moves on — but the
/// error is returned rather than swallowed, because the admin page's test-send
/// has nothing else to show. A wrong ntfy topic used to be silent: the request
/// succeeded, the status code said 404, and nothing looked at it.
pub async fn send(client: &Client, notifier: &Notifier, t: &Transition) -> Result<(), String> {
    match &notifier.kind {
        NotifierKind::Webhook { url } => send_webhook(client, url, t).await,
        NotifierKind::Ntfy { topic, base_url } => {
            send_ntfy(client, base_url.as_deref(), topic, t).await
        }
        NotifierKind::Slack { webhook_url } => send_slack(client, webhook_url, t).await,
        NotifierKind::Pagerduty { routing_key } => send_pagerduty(client, routing_key, t).await,
        NotifierKind::Email(m) => send_email(m, t).await,
    }
}

const DISPATCH_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(10);

/// POST a simple JSON alert payload to a webhook.
async fn send_webhook(client: &Client, url: &str, t: &Transition) -> Result<(), String> {
    let payload = serde_json::json!({
        "check_id": t.check_id,
        "from": t.from.as_str(),
        "to": t.to.as_str(),
    });
    delivered(client.post(url).json(&payload)).await
}

/// The topic hangs off the server, so no `base_url` means the public one.
fn ntfy_url(base_url: Option<&str>, topic: &str) -> String {
    format!(
        "{}/{topic}",
        base_url.unwrap_or(NTFY_DEFAULT).trim_end_matches('/')
    )
}

/// POST a transition to an NTFY topic.
async fn send_ntfy(
    client: &Client,
    base_url: Option<&str>,
    topic: &str,
    t: &Transition,
) -> Result<(), String> {
    let title = format!("Check {} → {}", t.check_id, t.to.as_str());
    let body = format!(
        "Status changed from {} to {}",
        t.from.as_str(),
        t.to.as_str()
    );
    delivered(
        client
            .post(ntfy_url(base_url, topic))
            .header("Title", title)
            .body(body),
    )
    .await
}

/// Slack incoming webhooks want `text`; everything else is optional.
async fn send_slack(client: &Client, url: &str, t: &Transition) -> Result<(), String> {
    let text = format!("*{}* {} → {}", t.check_id, t.from.as_str(), t.to.as_str());
    delivered(client.post(url).json(&serde_json::json!({ "text": text }))).await
}

const PAGERDUTY_EVENTS: &str = "https://events.pagerduty.com/v2/enqueue";

/// Events API v2. A recovery is a `resolve` on the same `dedup_key`, so
/// PagerDuty closes the incident the DOWN opened rather than leaving a human to
/// close it by hand — and a flapping check reuses one incident instead of
/// spraying twenty.
async fn send_pagerduty(client: &Client, routing_key: &str, t: &Transition) -> Result<(), String> {
    delivered(
        client
            .post(PAGERDUTY_EVENTS)
            .json(&pagerduty_event(routing_key, t)),
    )
    .await
}

fn pagerduty_event(routing_key: &str, t: &Transition) -> serde_json::Value {
    let up = t.to == Status::Up;
    let mut event = serde_json::json!({
        "routing_key": routing_key,
        "dedup_key": t.check_id,
        "event_action": if up { "resolve" } else { "trigger" },
    });
    // Required on a trigger, ignored on a resolve. UNKNOWN is a warning: the
    // probe could not say, which is not the same as a service being down.
    if !up {
        event["payload"] = serde_json::json!({
            "summary": format!("{} is {}", t.check_id, t.to.as_str()),
            "source": t.check_id,
            "severity": if t.to == Status::Down { "error" } else { "warning" },
        });
    }
    event
}

/// Send one mail over SMTP.
///
/// ponytail: a fresh connection per notification, no pool. An alert is rare and
/// a pooled SMTP session held open for days is exactly the thing that goes
/// stale unnoticed; pool it if a deployment ever pages in bursts.
async fn send_email(m: &MailConfig, t: &Transition) -> Result<(), String> {
    let addr = |raw: &str| raw.parse::<Mailbox>().map_err(|e| format!("{raw:?}: {e}"));
    let message = Message::builder()
        .from(addr(&m.from)?)
        .to(addr(&m.to)?)
        .subject(format!("{} is {}", t.check_id, t.to.as_str()))
        .body(format!(
            "Status changed from {} to {}.\n",
            t.from.as_str(),
            t.to.as_str()
        ))
        .map_err(|e| e.to_string())?;

    let mut builder = match m.tls {
        MailTls::Starttls => AsyncSmtpTransport::<Tokio1Executor>::starttls_relay(&m.host),
        MailTls::Implicit => AsyncSmtpTransport::<Tokio1Executor>::relay(&m.host),
        MailTls::None => Ok(AsyncSmtpTransport::<Tokio1Executor>::builder_dangerous(
            &m.host,
        )),
    }
    .map_err(|e| e.to_string())?;
    if let Some(port) = m.port {
        builder = builder.port(port);
    }
    if let (Some(user), Some(pass)) = (&m.username, &m.password) {
        builder = builder.credentials(Credentials::new(user.clone(), pass.clone()));
    }
    let transport = builder
        .timeout(Some(DISPATCH_TIMEOUT))
        .build::<Tokio1Executor>();

    match tokio::time::timeout(DISPATCH_TIMEOUT, transport.send(message)).await {
        Ok(Ok(_)) => Ok(()),
        Ok(Err(e)) => Err(e.to_string()),
        Err(_) => Err(format!("no reply within {}s", DISPATCH_TIMEOUT.as_secs())),
    }
}

/// One place decides what "delivered" means. A non-2xx is a failure: an ntfy
/// server answers 404 for a topic you are not allowed to publish to, and
/// treating that as success is what made a typo undetectable.
async fn delivered(req: reqwest::RequestBuilder) -> Result<(), String> {
    match tokio::time::timeout(DISPATCH_TIMEOUT, req.send()).await {
        Ok(Ok(r)) if r.status().is_success() => Ok(()),
        Ok(Ok(r)) => Err(format!("the endpoint answered {}", r.status())),
        // reqwest's Display is already a sentence ("error sending request for
        // url (…): connection refused"); the source chain adds the cause.
        Ok(Err(e)) => Err(match std::error::Error::source(&e) {
            Some(cause) => format!("{e}: {cause}"),
            None => e.to_string(),
        }),
        Err(_) => Err(format!("no reply within {}s", DISPATCH_TIMEOUT.as_secs())),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn webhook(name: &str, url: &str) -> Notifier {
        Notifier {
            name: name.into(),
            kind: NotifierKind::Webhook { url: url.into() },
            default: true,
        }
    }

    #[test]
    fn a_notifier_round_trips_flat() {
        let notifier = webhook("oncall", "https://example.com/webhook");
        let json = serde_json::to_string(&notifier).unwrap();
        // Flat on the wire: the kind's tag sits beside name and default rather
        // than nesting, so the form and the config file read the same.
        assert_eq!(
            json,
            r#"{"name":"oncall","type":"webhook","url":"https://example.com/webhook","default":true}"#
        );
        assert_eq!(serde_json::from_str::<Notifier>(&json).unwrap(), notifier);
    }

    #[test]
    fn ntfy_defaults_to_the_public_server() {
        assert_eq!(ntfy_url(None, "my-topic"), "https://ntfy.sh/my-topic");
        assert_eq!(
            ntfy_url(Some("https://notify.example.com/"), "my-topic"),
            "https://notify.example.com/my-topic"
        );
    }

    fn transition(check: &str, from: Status, to: Status) -> Transition {
        Transition {
            check_id: check.into(),
            from,
            to,
        }
    }

    /// A recovery must resolve the incident the outage opened, not open a
    /// second one — which is what the shared dedup key buys, and the reason
    /// `event_action` is derived from the state rather than always `trigger`.
    #[test]
    fn pagerduty_triggers_on_down_and_resolves_the_same_incident() {
        let down = pagerduty_event("rk", &transition("api", Status::Up, Status::Down));
        assert_eq!(down["event_action"], "trigger");
        assert_eq!(down["dedup_key"], "api");
        assert_eq!(down["payload"]["severity"], "error");

        let up = pagerduty_event("rk", &transition("api", Status::Down, Status::Up));
        assert_eq!(up["event_action"], "resolve");
        assert_eq!(up["dedup_key"], down["dedup_key"], "same incident");
        // Ignored by the API on a resolve, and PagerDuty rejects a trigger
        // without it — so its presence is exactly the trigger/resolve split.
        assert!(up.get("payload").is_none());

        // A probe that could not decide is not a hard outage.
        let unknown = pagerduty_event("rk", &transition("api", Status::Up, Status::Unknown));
        assert_eq!(unknown["payload"]["severity"], "warning");
    }

    /// Email is the one kind with a nested config, so the flat wire shape and
    /// the defaults are worth pinning: `tls` absent must mean STARTTLS, never
    /// plaintext.
    #[test]
    fn email_reads_flat_and_defaults_to_starttls() {
        let n: Notifier = serde_json::from_str(
            r#"{"name":"ops","type":"email","host":"smtp.example.com",
                "from":"warden@example.com","to":"ops@example.com"}"#,
        )
        .unwrap();
        let NotifierKind::Email(m) = &n.kind else {
            panic!("expected an email notifier, got {:?}", n.kind);
        };
        assert_eq!(m.tls, MailTls::Starttls);
        assert_eq!(m.port, None, "absent means the standard port for the mode");
        n.validate().unwrap();
        assert!(serde_json::to_string(&n)
            .unwrap()
            .contains(r#""type":"email","host""#));
    }

    #[test]
    fn email_rejects_what_cannot_be_delivered() {
        let mail = |m: MailConfig| Notifier {
            name: "ops".into(),
            kind: NotifierKind::Email(m),
            default: true,
        };
        let good = MailConfig {
            host: "smtp.example.com".into(),
            port: None,
            username: None,
            password: None,
            from: "warden@example.com".into(),
            to: "ops@example.com".into(),
            tls: MailTls::Starttls,
        };
        mail(good.clone()).validate().unwrap();
        for (why, broken) in [
            (
                "no host",
                MailConfig {
                    host: String::new(),
                    ..good.clone()
                },
            ),
            (
                "unusable recipient",
                MailConfig {
                    to: "ops at example.com".into(),
                    ..good.clone()
                },
            ),
            (
                // Half-configured auth fails at the relay, and a form is the
                // only place that is cheap to say so.
                "username with no password",
                MailConfig {
                    username: Some("warden".into()),
                    ..good.clone()
                },
            ),
        ] {
            assert!(mail(broken).validate().is_err(), "{why} must be rejected");
        }
    }

    /// A cleared optional field arrives as "", which must read as absence: an
    /// empty username would otherwise pair with no password and fail auth.
    #[test]
    fn sanitizing_email_clears_blank_credentials() {
        let n = Notifier {
            name: " ops ".into(),
            kind: NotifierKind::Email(MailConfig {
                host: " smtp.example.com ".into(),
                port: Some(2525),
                username: Some("  ".into()),
                password: Some(String::new()),
                from: " warden@example.com ".into(),
                to: " ops@example.com ".into(),
                tls: MailTls::None,
            }),
            default: true,
        }
        .sanitized();
        assert_eq!(n.name, "ops");
        let NotifierKind::Email(m) = &n.kind else {
            unreachable!()
        };
        assert_eq!(m.host, "smtp.example.com");
        assert_eq!(m.username, None);
        assert_eq!(m.password, None);
        assert_eq!(m.from, "warden@example.com");
        n.validate().unwrap();
    }

    /// A notifier written before routing existed has neither a name nor a
    /// default flag. It must keep paging on upgrade, or a release silently
    /// stops every alert.
    #[test]
    fn a_notifier_from_before_routing_still_receives_default_alerts() {
        let legacy: Notifier =
            serde_json::from_str(r#"{"type":"webhook","url":"https://example.com/hook"}"#).unwrap();
        assert!(legacy.default, "absent means default, not muted");
        assert_eq!(legacy.name, "");
        assert_eq!(targets(std::slice::from_ref(&legacy), &[]).len(), 1);
    }

    #[test]
    fn routing_picks_by_name_and_falls_back_to_the_defaults() {
        let all = vec![
            webhook("oncall", "https://example.com/a"),
            Notifier {
                default: false,
                ..webhook("database-team", "https://example.com/b")
            },
        ];
        // No routing named: only the defaults.
        let names = |t: Vec<&Notifier>| t.iter().map(|n| n.name.clone()).collect::<Vec<_>>();
        assert_eq!(names(targets(&all, &[])), ["oncall"]);
        // Named: exactly those, default flag irrelevant.
        assert_eq!(
            names(targets(&all, &["database-team".into()])),
            ["database-team"]
        );
        assert_eq!(
            names(targets(&all, &["oncall".into(), "database-team".into()])).len(),
            2
        );
        // A name nothing answers to reaches nobody — deliberately not a silent
        // fall back to the defaults, which would page the wrong people.
        assert!(targets(&all, &["gone".into()]).is_empty());
    }
}
