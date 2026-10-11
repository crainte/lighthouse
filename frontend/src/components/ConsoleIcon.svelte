<script lang="ts">
  let { name, size = 18 }: { name: string; size?: number } = $props();
</script>

<svg class="icon" width={size} height={size} viewBox="0 0 24 24" aria-hidden="true" focusable="false">
  {#if name === 'overview'}
    <rect x="3" y="3" width="7" height="7" rx="1" /><rect x="14" y="3" width="7" height="7" rx="1" /><rect x="3" y="14" width="7" height="7" rx="1" /><rect x="14" y="14" width="7" height="7" rx="1" />
  {:else if name === 'checks'}
    <path d="M2 12h5l3-8 4 16 3-8h5" />
  {:else if name === 'incidents' || name === 'alert'}
    <path d="m12 3 10 18H2Z" /><path d="M12 9v5m0 3v.1" />
  {:else if name === 'agents' || name === 'data'}
    <rect x="4" y="3" width="16" height="7" rx="2" /><rect x="4" y="14" width="16" height="7" rx="2" /><path d="M8 6.5h.01M8 17.5h.01M12 6.5h5M12 17.5h5" />
  {:else if name === 'cluster' || name === 'region-tree'}
    <circle cx="12" cy="5" r="3" /><circle cx="5" cy="19" r="3" /><circle cx="19" cy="19" r="3" /><path d="m10 8-4 8m8-8 4 8M8 19h8" />
  {:else if name === 'results' || name === 'status'}
    <rect x="5" y="3" width="14" height="18" rx="2" /><path d="M9 8h6M9 12h6m-6 4h4" />
  {:else if name === 'credentials'}
    <circle cx="8" cy="8" r="5" /><path d="m12 12 9 9m-5-5 3-3m-6 0 3-3" />
  {:else if name === 'alerts' || name === 'notifications'}
    <path d="M5 17h14l-2-4V9a5 5 0 0 0-10 0v4Zm5 4h4" />
  {:else if name === 'maintenance'}
    <circle cx="12" cy="12" r="9" /><path d="M12 7v6h4" />
  {:else if name === 'arrow'}
    <path d="M5 12h14m-5-5 5 5-5 5" />
  {:else if name === 'station'}
    <path d="m9 20 1-10h4l1 10M8 20h8M9 7h6l-3-3Zm1 0v3m4-3v3M4 7h2m12 0h2" />
  {:else if name === 'menu'}
    <path d="M4 6h16M4 12h16M4 18h16" />
  {:else if name === 'search'}
    <circle cx="10" cy="10" r="6" /><path d="m15 15 5 5" />
  {/if}
</svg>

<style>
  .icon { display: block; flex-shrink: 0; fill: none; stroke: currentColor; stroke-width: 1.5; stroke-linecap: round; stroke-linejoin: round; }
</style>
