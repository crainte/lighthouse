import { render } from '@testing-library/svelte';
import { tick } from 'svelte';
import { afterEach, describe, expect, it, vi } from 'vitest';

import LighthouseHero from './LighthouseHero.svelte';

describe('LighthouseHero', () => {
  afterEach(() => {
    vi.unstubAllGlobals();
  });

  it('sweeps once when session storage is unavailable and settles for reduced motion', async () => {
    let reduced = false;
    let onChange: ((event: Event) => void) | undefined;
    vi.stubGlobal('matchMedia', () => ({
      get matches() {
        return reduced;
      },
      addEventListener: (_type: string, listener: (event: Event) => void) => {
        onChange = listener;
      },
      removeEventListener: vi.fn(),
    }));
    vi.spyOn(Storage.prototype, 'getItem').mockImplementation(() => {
      throw new Error('blocked');
    });
    vi.spyOn(Storage.prototype, 'setItem').mockImplementation(() => {
      throw new Error('blocked');
    });

    const { container } = render(LighthouseHero);
    const beam = container.querySelector('.beam');
    expect(beam).toHaveClass('sweeping');

    reduced = true;
    onChange?.(new Event('change'));
    await tick();
    expect(beam).not.toHaveClass('sweeping');

    reduced = false;
    onChange?.(new Event('change'));
    await tick();
    expect(beam).not.toHaveClass('sweeping');
  });
});
