<script lang="ts">
  import { atBound, stepValue } from '../lib/number';

  let {
    value = $bindable(),
    min,
    max,
    step,
    placeholder,
    ariaLabel,
    readonly = false,
    disabled = false,
    mono = false,
    onchanged,
  }: {
    /** Null is "the field is empty", which is not the same as zero: the admin
     *  forms use empty to mean "inherit the coordinator default". */
    value: number | null;
    min?: number;
    max?: number;
    step?: number;
    placeholder?: string;
    /** For the fields whose label is not adjacent, e.g. a quorum box. */
    ariaLabel?: string;
    readonly?: boolean;
    disabled?: boolean;
    mono?: boolean;
    /** Fired after a change from either the keyboard or the arrows, for the
     *  panels that track a dirty row alongside the value. */
    onchanged?: () => void;
  } = $props();

  const opts = $derived({ min, max, step });
  const inert = $derived(readonly || disabled);
  const upDone = $derived(inert || atBound(value, 1, opts));
  const downDone = $derived(inert || atBound(value, -1, opts));

  // Hold-to-repeat. Both handles live here rather than in the press handler so
  // the teardown below can always reach them: a section collapsing mid-hold
  // removes the button without ever firing pointerup, and an interval still
  // counting against a detached field is a value that changes on its own.
  let delay: ReturnType<typeof setTimeout> | null = null;
  let repeat: ReturnType<typeof setInterval> | null = null;

  function stop() {
    if (delay !== null) clearTimeout(delay);
    if (repeat !== null) clearInterval(repeat);
    delay = null;
    repeat = null;
  }

  function bump(dir: 1 | -1) {
    if (inert) return;
    value = stepValue(value, dir, opts);
    onchanged?.();
  }

  function press(dir: 1 | -1) {
    stop();
    bump(dir);
    // Long enough that a normal click is one step, short enough that holding
    // does not feel broken.
    delay = setTimeout(() => {
      repeat = setInterval(() => {
        if (atBound(value, dir, opts)) {
          stop();
          return;
        }
        bump(dir);
      }, 60);
    }, 400);
  }

  $effect(() => stop);
</script>

<div class="num" class:readonly={inert}>
  <input
    type="number"
    class:mono
    bind:value
    oninput={() => onchanged?.()}
    {min}
    {max}
    {step}
    {placeholder}
    {readonly}
    {disabled}
    aria-label={ariaLabel}
  />
  <!-- The input is already operable by keyboard and announced by screen
       readers, so these are decoration: hidden from the accessibility tree and
       out of the tab order, or every numeric field would cost three tab stops
       instead of one. An explicit label on each would never be announced
       either, which is what the svelte-ignore lines below are about. -->
  <span class="num-steps" aria-hidden="true">
    <!-- svelte-ignore a11y_consider_explicit_label -->
    <button
      type="button"
      tabindex="-1"
      class="num-step"
      disabled={upDone}
      onpointerdown={() => press(1)}
      onpointerup={stop}
      onpointerleave={stop}
      onpointercancel={stop}
      onblur={stop}
    >
      <svg viewBox="0 0 10 6" width="9" height="6"
        ><path d="M1 5l4-4 4 4" fill="none" stroke="currentColor" stroke-width="1.6"
          stroke-linecap="round" stroke-linejoin="round" /></svg
      >
    </button>
    <!-- svelte-ignore a11y_consider_explicit_label -->
    <button
      type="button"
      tabindex="-1"
      class="num-step"
      disabled={downDone}
      onpointerdown={() => press(-1)}
      onpointerup={stop}
      onpointerleave={stop}
      onpointercancel={stop}
      onblur={stop}
    >
      <svg viewBox="0 0 10 6" width="9" height="6"
        ><path d="M1 1l4 4 4-4" fill="none" stroke="currentColor" stroke-width="1.6"
          stroke-linecap="round" stroke-linejoin="round" /></svg
      >
    </button>
  </span>
</div>
