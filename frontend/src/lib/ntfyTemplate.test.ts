import { describe, expect, it } from 'vitest';
import { ntfyPreview } from './ntfyTemplate';

describe('ntfy template preview', () => {
  it('shows the current alert wording until a channel customizes it', () => {
    expect(ntfyPreview('', '')).toEqual({
      title: 'Check API → down',
      message: 'Status changed from up to down',
    });
  });

  it('updates both parts of the preview using the documented tokens', () => {
    expect(ntfyPreview('Alert: {name} is {to}', '{name}: {from} -> {to}')).toEqual({
      title: 'Alert: API is down',
      message: 'API: up -> down',
    });
  });

  it('flags unsupported placeholders instead of displaying an unsendable preview', () => {
    expect(ntfyPreview('Alert {status}', '')).toEqual({
      error: 'Unknown placeholder {status}; use {name}, {from}, or {to}',
    });
  });

  it('rejects C1 control characters in titles like the server does', () => {
    expect(ntfyPreview('Alert\u0085title', '')).toEqual({
      error: 'ntfy title cannot contain newlines or control characters',
    });
  });
});
