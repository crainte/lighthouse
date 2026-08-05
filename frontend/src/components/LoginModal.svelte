<script lang="ts">
  import { login } from '../lib/admin';

  let { onsuccess, oncancel }: { onsuccess: () => void; oncancel: () => void } = $props();

  let password = $state('');
  let error = $state<string | null>(null);
  let busy = $state(false);
  let field = $state<HTMLInputElement | null>(null);

  // Focus once the input exists. `autofocus` is the a11y-warned shortcut and
  // doesn't re-focus after a failed attempt clears the field.
  $effect(() => {
    field?.focus();
  });

  async function submit(e: SubmitEvent) {
    e.preventDefault();
    if (busy) return;
    busy = true;
    error = null;
    try {
      await login(password);
      onsuccess();
    } catch (err) {
      // Rate limiting and bad passwords both arrive as server copy; showing our
      // own wording would hide "too many attempts" behind "invalid password".
      error = err instanceof Error ? err.message : String(err);
      password = '';
      field?.focus();
    } finally {
      busy = false;
    }
  }

  function onBackdrop(e: MouseEvent) {
    if ((e.target as HTMLElement).classList.contains('modal-backdrop')) oncancel();
  }

  function onKeydown(e: KeyboardEvent) {
    if (e.key === 'Escape') oncancel();
  }
</script>

<svelte:window onkeydown={onKeydown} />

<!-- svelte-ignore a11y_click_events_have_key_events a11y_no_static_element_interactions -->
<div class="modal-backdrop" onclick={onBackdrop} role="dialog" aria-modal="true" tabindex="-1">
  <div class="modal">
    <div class="modal-header">
      <h2>Admin sign in</h2>
      <button class="close-btn" onclick={oncancel} aria-label="Close">&times;</button>
    </div>

    <form class="modal-body" onsubmit={submit}>
      <label class="panel-title" for="admin-password">Password</label>
      <input
        id="admin-password"
        bind:this={field}
        bind:value={password}
        type="password"
        autocomplete="current-password"
      />
      {#if error}
        <p class="error">{error}</p>
      {/if}
      <button class="submit" type="submit" disabled={busy}>
        {busy ? 'Signing in…' : 'Sign in'}
      </button>
    </form>
  </div>
</div>

<style>
  .modal-backdrop {
    position: fixed;
    inset: 0;
    z-index: 1000;
    background: rgba(1, 4, 9, 0.72);
    display: flex;
    align-items: flex-start;
    justify-content: center;
    padding: 12vh 3vw;
    overflow-y: auto;
  }

  /* Narrow: one field. The wide detail dialog shape would read as a form with
     most of its fields missing. */
  .modal {
    background: var(--bg-card, #161b22);
    border: 1px solid var(--border, #30363d);
    border-radius: 12px;
    width: min(380px, 100%);
    box-shadow: 0 16px 48px rgba(0, 0, 0, 0.6);
  }

  .modal-header {
    display: flex;
    align-items: center;
    justify-content: space-between;
    gap: 1rem;
    padding: 0.9rem 1.2rem;
    border-bottom: 1px solid var(--border, #30363d);
  }
  .modal-header h2 {
    margin: 0;
    font-size: 1.05rem;
    font-weight: 600;
    color: var(--text, #e6edf3);
  }

  .close-btn {
    background: none;
    border: none;
    color: var(--text-dim, #8b949e);
    font-size: 1.5rem;
    cursor: pointer;
    padding: 0;
    line-height: 1;
  }
  .close-btn:hover {
    color: var(--text, #e6edf3);
  }

  .modal-body {
    display: flex;
    flex-direction: column;
    gap: 0.5rem;
    padding: 1.1rem 1.2rem 1.2rem;
  }

  .panel-title {
    font-size: 0.72rem;
    text-transform: uppercase;
    letter-spacing: 0.04em;
    color: var(--text-dim, #8b949e);
  }

  input {
    background: var(--bg, #0d1117);
    border: 1px solid var(--border, #30363d);
    border-radius: 6px;
    color: var(--text, #e6edf3);
    font: inherit;
    font-size: 0.85rem;
    padding: 0.42rem 0.6rem;
    width: 100%;
  }
  input:focus {
    outline: none;
    border-color: var(--accent, #58a6ff);
  }
  input:disabled {
    opacity: 0.6;
  }

  .submit {
    margin-top: 0.25rem;
    background: #1f6feb22;
    border: 1px solid var(--accent, #58a6ff);
    border-radius: 6px;
    color: var(--accent, #58a6ff);
    font: inherit;
    font-size: 0.8rem;
    font-weight: 600;
    padding: 0.42rem 0.6rem;
    cursor: pointer;
  }
  .submit:hover:not(:disabled) {
    background: #1f6feb33;
  }
  .submit:disabled {
    cursor: default;
    opacity: 0.55;
  }

  .error {
    margin: 0;
    font-size: 0.8rem;
    color: var(--down, #f85149);
  }
</style>
