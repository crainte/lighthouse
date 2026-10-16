import type { CheckKind } from '../../lib/api';
import type {
  AdminCheck,
  DefaultStatus,
  MatchMode,
  RollupPolicy,
  Threshold,
} from '../../lib/admin';

// Number inputs go empty rather than to zero when cleared, so the draft holds
// nulls and the guards below refuse them; AdminCheck itself stays honest.
// `group` and the optional strings are non-optional here because `bind:value`
// needs a string, and `position` is absent because the server owns ordering.
export type ThresholdKind = 'any' | 'majority' | 'all' | 'quorum';
export type ThresholdDraft = { kind: ThresholdKind; quorum: number | null };
export type PolicyDraft = { degraded_when: ThresholdDraft; down_when: ThresholdDraft };
export type OptionalBoolean = '' | 'true' | 'false';

export type Draft = Omit<
  AdminCheck,
  | 'id'
  | 'timeout_secs'
  | 'expect'
  | 'group'
  | 'eval'
  | 'description'
  | 'image'
  | 'tag'
  | 'position'
  | 'dns'
  | 'ping'
  | 'cert_warning_days'
  | 'grace_secs'
  | 'retry_interval_secs'
  | 'down_interval_secs'
  | 'placement'
  | 'rollup'
  | 'min_incident_secs'
  | 'min_recovery_secs'
  | 'notify_degraded'
  | 'degraded_is_downtime'
> & {
  timeout_secs: number | null;
  expect_status: number | null;
  expect_body: string;
  group: string;
  eval: string;
  description: string;
  image: string;
  tag: string;
  dns: {
    mode: 'standard' | 'dnssec';
    nameserver: string;
    record: string;
    match_type: MatchMode;
    expected: string[];
  };
  ping: { ip_type: string; count: number | null };
  cert_warning_days: number | null;
  grace_secs: number | null;
  failures_before_down: number | null;
  // Blank means omitted — the server derives a value from the schedule and
  // timeout — while heartbeat checks never read either, since nothing is
  // retried between pings.
  retry_interval_secs: number | null;
  down_interval_secs: number | null;
  rollup: { zone: PolicyDraft | null; region: PolicyDraft | null; global: PolicyDraft | null };
  min_incident_secs: number | null;
  min_recovery_secs: number | null;
  notify_degraded: OptionalBoolean;
  degraded_is_downtime: OptionalBoolean;
  /** Upstream checks. A DOWN here joins the upstream's open incident rather
   *  than opening a second one for the same root cause. */
  depends_on: string[];
  // Same reason as every other number here: cleared reads as empty, not zero.
  placement: { mode: 'zone' | 'region'; zones: string[]; regions: string[]; count: number | null };
};

export const KINDS: CheckKind[] = ['http', 'tcp', 'dns', 'icmp', 'ssl', 'heartbeat', 'openmetrics'];
export const MATCH_MODES: MatchMode[] = ['any', 'all'];
export const DEFAULT_STATUSES: DefaultStatus[] = ['up', 'down', 'unknown'];
export const DNS_RECORDS = ['A', 'AAAA', 'CNAME', 'MX', 'TXT', 'NS'];
export const IP_TYPES = ['any', 'ipv4', 'ipv6'];
export const THRESHOLD_KINDS: ThresholdKind[] = ['any', 'majority', 'all', 'quorum'];
export const POLICY_EDGES: { key: keyof PolicyDraft; label: string }[] = [
  { key: 'degraded_when', label: 'Degraded' },
  { key: 'down_when', label: 'DOWN' },
];

export function thresholdDraft(value: Threshold): ThresholdDraft {
  return typeof value === 'string'
    ? { kind: value, quorum: 1 }
    : { kind: 'quorum', quorum: value.quorum };
}

export function policyDraft(value: RollupPolicy): PolicyDraft {
  return {
    degraded_when: thresholdDraft(value.degraded_when),
    down_when: thresholdDraft(value.down_when),
  };
}

export function thresholdValue(value: ThresholdDraft): Threshold {
  return value.kind === 'quorum' ? { quorum: value.quorum ?? 1 } : value.kind;
}

export function policyValue(value: PolicyDraft): RollupPolicy {
  return {
    degraded_when: thresholdValue(value.degraded_when),
    down_when: thresholdValue(value.down_when),
  };
}

export function optionalBoolean(value: boolean | undefined): OptionalBoolean {
  return value === undefined ? '' : (String(value) as OptionalBoolean);
}

export function blank(): Draft {
  return {
    name: '',
    type: 'http',
    target: '',
    hosts: [],
    host_match: 'all',
    cron: '*/30 * * * * *',
    timeout_secs: 10,
    group: '',
    latency_graph: false,
    expect_status: null,
    expect_body: '',
    enabled: true,
    visible: true,
    notify: true,
    notifiers: [],
    depends_on: [],
    eval: '',
    agents: [],
    description: '',
    image: '',
    tag: '',
    default_status: 'unknown',
    headers: [],
    allow_self_signed: false,
    dns: { mode: 'standard', nameserver: '', record: 'A', match_type: 'all', expected: [] },
    ping: { ip_type: 'any', count: 1 },
    metric: { name: '', labels: [] },
    cert_warning_days: 14,
    grace_secs: 300,
    failures_before_down: 1,
    retry_interval_secs: null,
    down_interval_secs: null,
    rollup: { zone: null, region: null, global: null },
    min_incident_secs: null,
    min_recovery_secs: null,
    notify_degraded: '',
    degraded_is_downtime: '',
    placement: { mode: 'zone', zones: [], regions: [], count: 1 },
  };
}

// Deep-copied field by field rather than spread: the draft must not alias the
// arrays inside the loaded check, or "cancel" would still have mutated them.
export function fromCheck(c: AdminCheck): Draft {
  const d = blank();
  return {
    ...d,
    name: c.name,
    type: c.type,
    target: c.target ?? '',
    hosts: [...(c.hosts ?? [])],
    host_match: c.host_match ?? d.host_match,
    cron: normaliseLegacyCron(c.cron ?? d.cron),
    timeout_secs: c.timeout_secs ?? d.timeout_secs,
    group: c.group ?? '',
    latency_graph: c.latency_graph ?? false,
    expect_status: c.expect?.status ?? null,
    expect_body: c.expect?.body_contains ?? '',
    enabled: c.enabled ?? true,
    visible: c.visible ?? true,
    notify: c.notify ?? true,
    notifiers: [...(c.notifiers ?? [])],
    depends_on: [...(c.depends_on ?? [])],
    eval: c.eval ?? '',
    agents: [...(c.agents ?? [])],
    description: c.description ?? '',
    image: c.image ?? '',
    tag: c.tag ?? '',
    default_status: c.default_status ?? d.default_status,
    headers: (c.headers ?? []).map((h) => ({ name: h.name, value: h.value })),
    allow_self_signed: c.allow_self_signed ?? false,
    dns: {
      mode: c.dns?.mode ?? 'standard',
      nameserver: c.dns?.nameserver ?? '',
      record: c.dns?.record ?? d.dns.record,
      match_type: c.dns?.match_type ?? d.dns.match_type,
      expected: [...(c.dns?.expected ?? [])],
    },
    ping: { ip_type: c.ping?.ip_type ?? d.ping.ip_type, count: c.ping?.count ?? d.ping.count },
    metric: {
      name: c.metric?.name ?? '',
      labels: (c.metric?.labels ?? []).map((l) => ({ name: l.name, value: l.value })),
    },
    cert_warning_days: c.cert_warning_days ?? d.cert_warning_days,
    grace_secs: c.grace_secs ?? d.grace_secs,
    failures_before_down: c.failures_before_down ?? d.failures_before_down,
    retry_interval_secs: c.retry_interval_secs ?? null,
    down_interval_secs: c.down_interval_secs ?? null,
    rollup: {
      zone: c.rollup?.zone ? policyDraft(c.rollup.zone) : null,
      region: c.rollup?.region ? policyDraft(c.rollup.region) : null,
      global: c.rollup?.global ? policyDraft(c.rollup.global) : null,
    },
    min_incident_secs: c.min_incident_secs ?? null,
    min_recovery_secs: c.min_recovery_secs ?? null,
    notify_degraded: optionalBoolean(c.notify_degraded),
    degraded_is_downtime: optionalBoolean(c.degraded_is_downtime),
    placement: {
      mode: c.placement?.mode ?? 'zone',
      zones: [...(c.placement?.zones ?? [])],
      regions: [...(c.placement?.regions ?? [])],
      count: c.placement?.count ?? c.placement?.per_zone ?? 1,
    },
  };
}

/** These map to unsigned integers server-side. A negative or fractional value
 *  can be typed past the `min` attribute and would fail JSON deserialization,
 *  surfacing a framework message instead of Lighthouse's own. */
/** Repair only the legacy one-unit-boundary cron forms. A step of sixty in
 * seconds/minutes is a start-of-next-unit schedule, never a valid cron step.
 * Match the server's narrow compatibility rule; reject other bad syntax there.
 */
export function normaliseLegacyCron(expr: string): string {
  const fields = expr.trim().split(/\s+/).filter(Boolean);
  const minute = fields.length === 5 ? 0 : fields.length === 6 ? 1 : -1;
  if (minute === -1) return expr.trim();
  if (fields.length === 6 && fields[0] === '*/60') fields[0] = '0';
  if (fields[minute] === '*/60') fields[minute] = '0';
  return fields.join(' ');
}

export function whole(v: number | null, min: number): boolean {
  return v !== null && Number.isInteger(v) && v >= min;
}

export function defaultPolicy(level: 'zone' | 'region' | 'global'): PolicyDraft {
  if (level === 'zone') return policyDraft({ degraded_when: 'all', down_when: 'all' });
  if (level === 'region') return policyDraft({ degraded_when: 'any', down_when: 'all' });
  return policyDraft({ degraded_when: 'any', down_when: 'majority' });
}

export function validPolicy(policy: PolicyDraft | null): boolean {
  if (!policy) return true;
  return [policy.degraded_when, policy.down_when].every(
    (threshold) => threshold.kind !== 'quorum' || whole(threshold.quorum, 1),
  );
}
