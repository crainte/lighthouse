// Illustrative outage preview in the editor. The server renders actual alerts.
const sample = { name: 'API', from: 'up', to: 'down' };

export const DEFAULT_NTFY_TITLE = 'Check {name} → {to}';
export const DEFAULT_NTFY_MESSAGE = 'Status changed from {from} to {to}';

function render(template: string): string {
  let rest = template;
  let result = '';
  while (rest.includes('{')) {
    const start = rest.indexOf('{');
    if (rest.slice(0, start).includes('}')) throw new Error('A closing brace has no opening brace');
    result += rest.slice(0, start);
    rest = rest.slice(start + 1);
    const end = rest.indexOf('}');
    if (end < 0) throw new Error('An opening brace has no closing brace');
    const token = rest.slice(0, end);
    if (!Object.hasOwn(sample, token)) {
      throw new Error(`Unknown placeholder {${token}}; use {name}, {from}, or {to}`);
    }
    result += sample[token as keyof typeof sample];
    rest = rest.slice(end + 1);
  }
  if (rest.includes('}')) throw new Error('A closing brace has no opening brace');
  return result + rest;
}

export function ntfyPreview(titleTemplate: string, messageTemplate: string) {
  try {
    const title = render(titleTemplate.trim() || DEFAULT_NTFY_TITLE);
    const message = render(messageTemplate.trim() || DEFAULT_NTFY_MESSAGE);
    if (/[\x00-\x1F\x7F-\x9F]/.test(title)) throw new Error('ntfy title cannot contain newlines or control characters');
    for (const [label, content, max] of [['title', title, 1024], ['message', message, 4096]] as const) {
      const bytes = new TextEncoder().encode(content).length;
      if (!bytes || bytes > max) throw new Error(`ntfy ${label} must be 1–${max} bytes`);
    }
    return { title, message };
  } catch (e) {
    return { error: e instanceof Error ? e.message : String(e) };
  }
}
