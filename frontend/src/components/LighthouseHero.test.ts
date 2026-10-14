import { cleanup, fireEvent, render, screen } from '@testing-library/svelte';
import { tick } from 'svelte';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { SEASONAL_STORAGE_KEY } from '../lib/seasonalAtmosphere';
import LighthouseHero from './LighthouseHero.svelte';

let reduced = false;
let onMotionChange: ((event: Event) => void) | undefined;

beforeEach(() => {
  localStorage.clear();
  sessionStorage.clear();
  reduced = false;
  onMotionChange = undefined;
  vi.stubGlobal('matchMedia', () => ({
    get matches() { return reduced; },
    addEventListener: (_type: string, listener: (event: Event) => void) => { onMotionChange = listener; },
    removeEventListener: vi.fn(),
  }));
});

afterEach(() => {
  cleanup();
  vi.useRealTimers();
  vi.restoreAllMocks();
  vi.unstubAllGlobals();
});

describe('LighthouseHero', () => {
  it('does not draw a rotating beam over image-backed scenes', () => {
    const { container } = render(LighthouseHero);
    expect(container.querySelector('.beam')).not.toBeInTheDocument();
    expect(container.querySelector('.lantern-aura')).not.toBeInTheDocument();
  });

  it('changes and persists automatic and manual seasonal treatments without replacing health', async () => {
    vi.useFakeTimers();
    vi.setSystemTime(new Date(2026, 6, 10, 12));
    const { container } = render(LighthouseHero, { props: { health: 'degraded' } });
    await tick();
    const scene = container.querySelector('.landscape');
    expect(scene).toHaveClass('summer', 'degraded');
    const photo = screen.getByTestId('photo-landscape');
    const summerSource = photo.querySelector('img')?.getAttribute('src');
    expect(photo).toHaveClass('summer', 'degraded');

    await fireEvent.click(screen.getByRole('button', { name: 'Seasonal atmosphere settings' }));
    await fireEvent.click(screen.getByRole('button', { name: 'On' }));
    await fireEvent.click(screen.getByRole('button', { name: 'Fall' }));
    expect(scene).toHaveClass('fall', 'degraded');
    expect(photo).toHaveClass('fall', 'degraded');
    const fallSource = photo.querySelector('img')?.getAttribute('src');
    expect(fallSource).not.toBe(summerSource);
    expect(JSON.parse(localStorage.getItem(SEASONAL_STORAGE_KEY) ?? '{}')).toMatchObject({ mode: 'on', season: 'fall' });

    await fireEvent.click(screen.getByRole('button', { name: 'Winter' }));
    expect(photo).toHaveClass('winter', 'degraded');
    expect(photo.querySelector('img')?.getAttribute('src')).not.toBe(fallSource);

    await fireEvent.click(screen.getByRole('button', { name: 'Spring' }));
    expect(photo).toHaveClass('spring', 'degraded');
    expect(photo.querySelector('img')?.getAttribute('src')).not.toBe(summerSource);

    await fireEvent.click(screen.getByRole('button', { name: 'Off' }));
    expect(scene).toHaveAttribute('data-season', 'off');
    expect(scene).toHaveClass('degraded');
    expect(photo).toHaveAttribute('data-season', 'off');
    expect(photo.querySelector('img')?.getAttribute('src')).not.toBe(summerSource);
  });

  it('suppresses all ambient behavior when reduced motion is requested', async () => {
    reduced = true;
    vi.useFakeTimers();
    vi.setSystemTime(new Date(2026, 11, 20, 12));
    vi.spyOn(Math, 'random').mockReturnValue(0);
    const { container } = render(LighthouseHero, { props: { health: 'down' } });
    await tick();
    const scene = container.querySelector('.landscape');
    expect(scene).toHaveClass('winter', 'down');
    expect(scene).not.toHaveClass('ambient');
  });

  it('closes settings with Escape and restores trigger focus', async () => {
    render(LighthouseHero);
    const trigger = screen.getByRole('button', { name: 'Seasonal atmosphere settings' });
    await fireEvent.click(trigger);
    expect(screen.getByRole('group', { name: 'Seasonal atmosphere settings' })).toBeInTheDocument();
    await fireEvent.keyDown(document, { key: 'Escape' });
    await tick();
    expect(screen.queryByRole('group', { name: 'Seasonal atmosphere settings' })).not.toBeInTheDocument();
    expect(trigger).toHaveFocus();
  });
});
