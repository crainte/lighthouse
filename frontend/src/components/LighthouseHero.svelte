<script lang="ts">
  import { onMount, tick } from 'svelte';
  import normalScene from '../assets/lighthouse-normal.png';
  import springScene from '../assets/lighthouse-spring.png';
  import summerScene from '../assets/lighthouse-summer.png';
  import fallScene from '../assets/lighthouse-fall.png';
  import winterScene from '../assets/lighthouse-winter.png';
  import {
    DEFAULT_SEASONAL_PREFERENCES,
    loadSeasonalPreferences,
    resolveSeason,
    saveSeasonalPreferences,
    type Season,
    type SeasonalMode,
    type SeasonalPreferences,
  } from '../lib/seasonalAtmosphere';

  export type LighthouseHealth = 'operational' | 'degraded' | 'down' | 'unknown';

  let { health = 'operational' }: { health?: LighthouseHealth } = $props();

  let recovering = $state(false);
  let previousHealth: LighthouseHealth | null = null;
  let recoveryTimer: ReturnType<typeof setTimeout> | undefined;
  let preferences = $state<SeasonalPreferences>({ ...DEFAULT_SEASONAL_PREFERENCES });
  let currentDate = $state(new Date());
  let reducedMotion = $state(false);
  let visible = $state(true);
  let settingsOpen = $state(false);
  let settingsButton = $state<HTMLButtonElement | null>(null);
  let settingsPanel = $state<HTMLDivElement | null>(null);
  let dateTimer: ReturnType<typeof window.setTimeout> | undefined;

  type Particle = {
    id: number;
    style: string;
  };

  let leaves = $state<Particle[]>([]);
  let snowflakes = $state<Particle[]>([]);

  const between = (min: number, max: number) => min + Math.random() * (max - min);

  function leafParticle(id: number): Particle {
    const x = between(280, 700);
    const y = between(-30, 90);
    const drift = between(-150, 160);
    const fall = between(125, 260);
    return {
      id,
      style: `--x:${x.toFixed(1)}px;--y:${y.toFixed(1)}px;--x1:${(drift * .22 + between(-45, 45)).toFixed(1)}px;--x2:${(drift * .48 + between(-60, 60)).toFixed(1)}px;--x3:${(drift * .74 + between(-45, 45)).toFixed(1)}px;--xe:${drift.toFixed(1)}px;--y1:${(fall * .18).toFixed(1)}px;--y2:${(fall * .43).toFixed(1)}px;--y3:${(fall * .7).toFixed(1)}px;--ye:${fall.toFixed(1)}px;--size:${between(1.2, 2.7).toFixed(2)};--turn:${between(250, 760).toFixed(0)}deg;--duration:${between(5.5, 12).toFixed(2)}s;--delay:${between(-12, 0).toFixed(2)}s;--alpha:${between(.38, .85).toFixed(2)}`,
    };
  }

  function snowParticle(id: number): Particle {
    const x = between(260, 790);
    const y = between(-80, 20);
    const drift = between(-95, 95);
    const fall = between(230, 340);
    return {
      id,
      style: `--x:${x.toFixed(1)}px;--y:${y.toFixed(1)}px;--x1:${(drift * .2 + between(-32, 32)).toFixed(1)}px;--x2:${(drift * .45 + between(-45, 45)).toFixed(1)}px;--x3:${(drift * .72 + between(-32, 32)).toFixed(1)}px;--xe:${drift.toFixed(1)}px;--y1:${(fall * .22).toFixed(1)}px;--y2:${(fall * .47).toFixed(1)}px;--y3:${(fall * .73).toFixed(1)}px;--ye:${fall.toFixed(1)}px;--size:${between(.7, 2.1).toFixed(2)};--duration:${between(6, 15).toFixed(2)}s;--delay:${between(-15, 0).toFixed(2)}s;--alpha:${between(.3, .9).toFixed(2)};--blur:${between(0, 1.1).toFixed(2)}px`,
    };
  }

  const activeSeason = $derived(resolveSeason(preferences, currentDate));
  const sceneImage = $derived(
    activeSeason === 'spring' ? springScene
      : activeSeason === 'summer' ? summerScene
        : activeSeason === 'fall' ? fallScene
          : activeSeason === 'winter' ? winterScene
            : normalScene,
  );
  const ambientMotion = $derived(!reducedMotion && visible);

  $effect(() => {
    const current = health;
    if (previousHealth && (previousHealth === 'degraded' || previousHealth === 'down') && current === 'operational') {
      recovering = true;
      clearTimeout(recoveryTimer);
      recoveryTimer = setTimeout(() => (recovering = false), 2600);
    } else if (current !== 'operational') {
      recovering = false;
    }
    previousHealth = current;
    return () => clearTimeout(recoveryTimer);
  });

  function persist(next: SeasonalPreferences) {
    preferences = next;
    saveSeasonalPreferences(localStorage, next);
  }

  function setMode(mode: SeasonalMode) {
    persist({ ...preferences, mode });
  }

  function setSeason(season: Season) {
    persist({ ...preferences, season });
  }

  function closeSettings(restoreFocus = false) {
    if (!settingsOpen) return;
    settingsOpen = false;
    if (restoreFocus) void tick().then(() => settingsButton?.focus());
  }

  onMount(() => {
    leaves = Array.from({ length: 18 }, (_, id) => leafParticle(id));
    snowflakes = Array.from({ length: 42 }, (_, id) => snowParticle(id));
    preferences = loadSeasonalPreferences(localStorage);
    currentDate = new Date();
    visible = document.visibilityState === 'visible';
    const motion = window.matchMedia('(prefers-reduced-motion: reduce)');
    reducedMotion = motion.matches;
    const refreshDate = () => {
      currentDate = new Date();
      const now = currentDate;
      const nextDay = new Date(now.getFullYear(), now.getMonth(), now.getDate() + 1);
      dateTimer = window.setTimeout(refreshDate, nextDay.getTime() - now.getTime() + 1000);
    };

    const handleVisibility = () => {
      visible = document.visibilityState === 'visible';
      if (visible) currentDate = new Date();
    };
    const handleMotion = () => {
      reducedMotion = motion.matches;
    };
    const handlePointer = (event: PointerEvent) => {
      const target = event.target as Node;
      if (settingsOpen && !settingsPanel?.contains(target) && !settingsButton?.contains(target)) closeSettings();
    };
    const handleKey = (event: KeyboardEvent) => {
      if (event.key === 'Escape') closeSettings(true);
    };

    document.addEventListener('visibilitychange', handleVisibility);
    document.addEventListener('pointerdown', handlePointer);
    document.addEventListener('keydown', handleKey);
    motion.addEventListener('change', handleMotion);
    const nextDay = new Date(currentDate.getFullYear(), currentDate.getMonth(), currentDate.getDate() + 1);
    dateTimer = window.setTimeout(refreshDate, nextDay.getTime() - currentDate.getTime() + 1000);

    return () => {
      if (dateTimer !== undefined) window.clearTimeout(dateTimer);
      document.removeEventListener('visibilitychange', handleVisibility);
      document.removeEventListener('pointerdown', handlePointer);
      document.removeEventListener('keydown', handleKey);
      motion.removeEventListener('change', handleMotion);
    };
  });
</script>

<div
  class="photo-landscape {health}"
  class:recovering
  class:fall={activeSeason === 'fall'}
  class:winter={activeSeason === 'winter'}
  class:spring={activeSeason === 'spring'}
  class:summer={activeSeason === 'summer'}
  data-testid="photo-landscape"
  data-season={activeSeason ?? 'off'}
  aria-hidden="true"
>
  <img src={sceneImage} alt="" />
  <div class="photo-tint"></div>
</div>

<svg
  class="landscape {health}"
  class:recovering
  class:fall={activeSeason === 'fall'}
  class:winter={activeSeason === 'winter'}
  class:spring={activeSeason === 'spring'}
  class:summer={activeSeason === 'summer'}
  class:ambient={ambientMotion}
  data-season={activeSeason ?? 'off'}
  viewBox="0 0 800 250"
  preserveAspectRatio="xMidYMid slice"
  aria-hidden="true"
  focusable="false"
>
  <defs>
    <linearGradient id="lighthouse-sky" x2="0" y2="1"><stop stop-color="#0a1625"/><stop offset=".67" stop-color="#112b36"/><stop offset="1" stop-color="#080f19"/></linearGradient>
    <linearGradient id="lighthouse-fall" x2="0" y2="1"><stop stop-color="#42372f" stop-opacity=".2"/><stop offset=".72" stop-color="#8b7046" stop-opacity=".16"/><stop offset="1" stop-color="#5b5d3c" stop-opacity=".08"/></linearGradient>
    <linearGradient id="lighthouse-winter" x2="0" y2="1"><stop stop-color="#193957" stop-opacity=".22"/><stop offset=".75" stop-color="#5f9ba2" stop-opacity=".14"/><stop offset="1" stop-color="#8eb4b5" stop-opacity=".06"/></linearGradient>
    <linearGradient id="lighthouse-spring" x2="0" y2="1"><stop stop-color="#16354a" stop-opacity=".12"/><stop offset=".72" stop-color="#5b9b8c" stop-opacity=".15"/><stop offset="1" stop-color="#728b68" stop-opacity=".07"/></linearGradient>
    <linearGradient id="lighthouse-summer" x2="0" y2="1"><stop stop-color="#061a32" stop-opacity=".25"/><stop offset=".72" stop-color="#39828d" stop-opacity=".14"/><stop offset="1" stop-color="#3c6973" stop-opacity=".06"/></linearGradient>
    <radialGradient id="lighthouse-air"><stop stop-color="#49878b" stop-opacity=".16"/><stop offset="1" stop-color="#234451" stop-opacity="0"/></radialGradient>
    <linearGradient id="lighthouse-sea" x2="0" y2="1"><stop stop-color="#1a3741" stop-opacity=".6"/><stop offset="1" stop-color="#080f19"/></linearGradient>
    <linearGradient id="lighthouse-stone"><stop stop-color="#233e49"/><stop offset=".45" stop-color="#6c7b7b"/><stop offset=".6" stop-color="#344d57"/><stop offset="1" stop-color="#152a35"/></linearGradient>
    <radialGradient id="lighthouse-amber"><stop stop-color="#e7b66b" stop-opacity=".2"/><stop offset="1" stop-color="#8a5f2a" stop-opacity="0"/></radialGradient>
    <radialGradient id="lighthouse-storm"><stop stop-color="#ee8389" stop-opacity=".13"/><stop offset="1" stop-color="#5b2733" stop-opacity="0"/></radialGradient>
    <radialGradient id="lighthouse-warning"><stop stop-color="#ffd0cf" stop-opacity=".95"/><stop offset=".18" stop-color="#ee8389" stop-opacity=".7"/><stop offset="1" stop-color="#ee8389" stop-opacity="0"/></radialGradient>
    <linearGradient id="lighthouse-fog"><stop stop-color="#74a8aa" stop-opacity="0"/><stop offset=".6" stop-color="#74a8aa" stop-opacity=".12"/><stop offset="1" stop-color="#74a8aa" stop-opacity="0"/></linearGradient>
  </defs>

  <g class="base-environment">
    <path fill="url(#lighthouse-sky)" d="M0 0h800v250H0z"/>
    <path d="M0 169Q160 165 300 168T800 168V250H0Z" fill="url(#lighthouse-sea)"/>
    <path d="M140 168h660" stroke="#678e94" opacity=".12"/>
    <path d="m30 172 96-8 80 4 70-9 60 10 90-2 64 7Z" fill="#0c1c28"/>
  </g>

  <g class="season-atmosphere">
    <path class="season-wash fall-wash" d="M0 0h800v250H0z" fill="url(#lighthouse-fall)"/>
    <path class="season-wash winter-wash" d="M0 0h800v250H0z" fill="url(#lighthouse-winter)"/>
    <path class="season-wash spring-wash" d="M0 0h800v250H0z" fill="url(#lighthouse-spring)"/>
    <path class="season-wash summer-wash" d="M0 0h800v250H0z" fill="url(#lighthouse-summer)"/>
  </g>

  <g class="ambient-details">
    <g class="leaves" fill="#b77a38">
      {#each leaves as leaf (leaf.id)}
        <path class="leaf" style={leaf.style} d="M0-2.5C2-2 3 0 0 3-3 0-2-2 0-2.5Z"/>
      {/each}
    </g>
    <g class="snowfall" fill="#e5f3f5">
      {#each snowflakes as snow (snow.id)}
        <circle class="snow" style={snow.style} r="1"/>
      {/each}
    </g>
  </g>

  <g class="shoreline-accents">
    <path class="fall-accent" d="m116 165 6-5 5 5m38 1 5-4 5 5m102-7 5-4 6 5" fill="none" stroke="#9a7650" stroke-width="2" opacity=".42"/>
    <path class="spring-accent" d="m104 167 7-6 6 5m48 1 7-6 6 5m90-4 8-6 7 6" fill="none" stroke="#688273" stroke-width="2" opacity=".38"/>
    <path class="winter-snow shoreline-snow" d="m39 171 83-7 31 2m330 39 34-10 28-9 35-2 27 4" fill="none" stroke="#b8ced1" stroke-width="2" opacity=".35"/>
  </g>

  <g class="lighthouse-and-rocks">
    <path d="m450 232 29-27 38-10 25-15 45-4 44 11 24-3 47 20 26 13 52 14 20 19H420Z" fill="#0b1520"/>
    <path d="m472 217 58-23 25-8 33-2 35 13-30-6-35 8-21-1-23 12Z" fill="#1d3039"/><path d="m616 199 24-6 27 12 31 20-40-13-26 1Z" fill="#152632"/>
    <path d="m568 189 10-89h24l12 91-21 7Z" fill="url(#lighthouse-stone)"/><path d="m580 102-6 86 11 4 2-90Z" fill="#a3aca0" opacity=".09"/>
    <path d="M575 98h31v5h-31Z" fill="#273c43"/><path d="M573 94h35v4h-35Z" fill="#8e9690" opacity=".6"/>
    <path d="M578 72h25v20h-25Z" fill="#112330"/><path d="M581 76h19v13h-19Z" fill="#d7c995" opacity=".56"/><path d="M584 77h12v10h-12Z" fill="#fff0c2"/>
    <path d="M581 74v17m9-17v17m9-17v17" stroke="#304147" stroke-width="2"/><path d="M575 72h31l-16-10Z" fill="#547078"/><path d="M575 72h31" stroke="#8c9d99"/>
    <path class="winter-snow roof-snow" d="m576 71 14-9 15 9-8-2-7-5-7 5Z" fill="#c3d5d5" opacity=".55"/>
    <path d="M590 57v5M571 91h39M572 88v7m6-7v7m24-7v7m7-7v7" stroke="#627c7c" stroke-width="1"/>
    <path d="M587 116h5v9h-5Zm-2 25h6v9h-6Zm-2 27h7v20h-7Z" fill="#0d202b"/><path d="M588 117h2v6h-2Z" fill="#e3ca8d" opacity=".45"/>
    <path d="M550 191h78v5h-78Z" fill="#2a3c42"/>
  </g>

  <g class="fog">
    <path d="M320 192q180-19 400 3l80 12q-220-17-480-1Z" fill="url(#lighthouse-fog)"/><path d="M220 217q200-15 480 11v10q-260-17-480-12Z" fill="url(#lighthouse-fog)"/>
  </g>
  <circle cx="344" cy="167" r="1.3" fill="#d1c193" opacity=".7"/><path d="M339 174h12m-16 5h19M712 180h37m-12 7h33M389 185h44" stroke="#9dbdb9" opacity=".09"/>

  <g class="operational-atmosphere">
    <ellipse class="air calm-air" cx="578" cy="140" rx="280" ry="180" fill="url(#lighthouse-air)"/>
    <ellipse class="air amber-air" cx="590" cy="120" rx="300" ry="170" fill="url(#lighthouse-amber)"/>
    <ellipse class="air storm-air" cx="590" cy="125" rx="320" ry="175" fill="url(#lighthouse-storm)"/>
    <g class="warning"><circle cx="590" cy="56" r="16" fill="url(#lighthouse-warning)"/><circle cx="590" cy="56" r="1.7" fill="#ffd9d7"/></g>
  </g>
</svg>

<button
  class="atmosphere-trigger"
  bind:this={settingsButton}
  aria-label="Seasonal atmosphere settings"
  aria-expanded={settingsOpen}
  aria-controls="seasonal-atmosphere-panel"
  onclick={() => (settingsOpen = !settingsOpen)}
>
  <span aria-hidden="true">◇</span>
</button>

{#if settingsOpen}
  <div class="atmosphere-panel" id="seasonal-atmosphere-panel" bind:this={settingsPanel} role="group" aria-label="Seasonal atmosphere settings">
    <div class="setting-head"><strong>Seasonal atmosphere</strong><span>Local to this browser</span></div>
    <div class="segmented" aria-label="Seasonal atmosphere mode">
      {#each ['off', 'auto', 'on'] as mode}
        <button class:active={preferences.mode === mode} aria-pressed={preferences.mode === mode} onclick={() => setMode(mode as SeasonalMode)}>{mode === 'on' ? 'On' : mode[0].toUpperCase() + mode.slice(1)}</button>
      {/each}
    </div>
    {#if preferences.mode === 'auto'}<p>Auto follows your browser’s local date.</p>{/if}
    {#if preferences.mode === 'on'}
      <div class="setting-label">Season</div>
      <div class="season-grid" aria-label="Season">
        {#each ['fall', 'winter', 'spring', 'summer'] as season}
          <button class:active={preferences.season === season} aria-pressed={preferences.season === season} onclick={() => setSeason(season as Season)}>{season[0].toUpperCase() + season.slice(1)}</button>
        {/each}
      </div>
    {/if}
  </div>
{/if}

<style>
  .photo-landscape { position: absolute; inset: 0; z-index: 0; overflow: hidden; pointer-events: none; background: #07111c; }
  .photo-landscape img { position: absolute; inset: 0; width: 100%; height: 100%; object-fit: cover; object-position: 50% 50%; filter: saturate(.9) contrast(1.03) brightness(.84); }
  .photo-landscape::before { content: ''; position: absolute; z-index: 1; inset: 0; background: linear-gradient(90deg, #07111c 0%, #07111cf5 27%, #07111c9c 43%, transparent 68%); }
  .photo-landscape::after { content: ''; position: absolute; z-index: 1; inset: 0; box-shadow: inset 0 0 48px #030a12cc; }
  .photo-tint { position: absolute; z-index: 2; inset: 0; opacity: 0; transition: opacity 1.2s ease; }
  .photo-landscape.degraded .photo-tint { opacity: 1; background: linear-gradient(90deg, transparent 44%, #7b501733); }
  .photo-landscape.down .photo-tint { opacity: 1; background: linear-gradient(90deg, transparent 38%, #8d26384d); }
  .photo-landscape.unknown img { filter: saturate(.5) contrast(1.02) brightness(.68); }
  .photo-landscape.recovering .photo-tint { animation: photo-recover 2.6s ease-out; background: #83d8c11f; }
  @keyframes photo-recover { 0%, 100% { opacity: 0; } 38% { opacity: 1; } }
  .landscape { position: absolute; inset: 0 0 0 auto; display: block; width: 65%; height: 100%; z-index: 1; mask-image: linear-gradient(90deg, transparent, #000 24%); pointer-events: none; }
  .base-environment, .season-atmosphere, .shoreline-accents, .lighthouse-and-rocks, .fog, .landscape > path { opacity: 0; }
  .season-wash, .fall-accent, .spring-accent, .winter-snow, .leaves, .snowfall { opacity: 0; }
  .fall .fall-wash, .fall .fall-accent, .winter .winter-wash, .winter .winter-snow, .spring .spring-wash, .spring .spring-accent, .summer .summer-wash { opacity: 1; }
  .summer .fog { opacity: .72; }
  .air, .warning { transition: opacity 1.6s ease; }
  .amber-air, .storm-air, .warning { opacity: 0; }
  .degraded .calm-air { opacity: .55; }
  .degraded .amber-air { opacity: 1; }
  .down .calm-air { opacity: .45; }
  .down .amber-air { opacity: .35; }
  .down .storm-air, .down .warning { opacity: 1; }
  .unknown .calm-air { opacity: .5; }
  .fall.ambient .leaves, .winter.ambient .snowfall { opacity: 1; }
  .leaf, .snow { transform-box: fill-box; transform-origin: center; opacity: 0; }
  .leaf { animation: leaf-drift var(--duration) var(--delay) ease-in-out infinite; filter: drop-shadow(0 1px 1px #30190ca8); }
  @keyframes leaf-drift {
    0% { opacity: 0; transform: translate(var(--x), var(--y)) rotate(0) scale(var(--size)); }
    8% { opacity: var(--alpha); }
    25% { transform: translate(calc(var(--x) + var(--x1)), calc(var(--y) + var(--y1))) rotate(calc(var(--turn) * .2)) scale(var(--size)); }
    49% { transform: translate(calc(var(--x) + var(--x2)), calc(var(--y) + var(--y2))) rotate(calc(var(--turn) * .52)) scale(calc(var(--size) * .8)); }
    72% { transform: translate(calc(var(--x) + var(--x3)), calc(var(--y) + var(--y3))) rotate(calc(var(--turn) * .76)) scale(var(--size)); }
    92% { opacity: var(--alpha); }
    100% { opacity: 0; transform: translate(calc(var(--x) + var(--xe)), calc(var(--y) + var(--ye))) rotate(var(--turn)) scale(calc(var(--size) * .7)); }
  }
  .snow { animation: snow-drift var(--duration) var(--delay) ease-in-out infinite; filter: blur(var(--blur)) drop-shadow(0 0 1.5px #e9f8ff); }
  @keyframes snow-drift {
    0% { opacity: 0; transform: translate(var(--x), var(--y)) scale(var(--size)); }
    7% { opacity: var(--alpha); }
    24% { transform: translate(calc(var(--x) + var(--x1)), calc(var(--y) + var(--y1))) scale(var(--size)); }
    48% { transform: translate(calc(var(--x) + var(--x2)), calc(var(--y) + var(--y2))) scale(calc(var(--size) * .85)); }
    73% { transform: translate(calc(var(--x) + var(--x3)), calc(var(--y) + var(--y3))) scale(var(--size)); }
    93% { opacity: var(--alpha); }
    100% { opacity: 0; transform: translate(calc(var(--x) + var(--xe)), calc(var(--y) + var(--ye))) scale(calc(var(--size) * .75)); }
  }
  .atmosphere-trigger { position: absolute; z-index: 101; top: 14px; right: 8px; display: grid; place-items: center; width: 34px; height: 34px; border: 1px solid transparent; border-radius: 6px; color: #74889a; background: #0b1621a6; opacity: .72; }
  .atmosphere-trigger:hover, .atmosphere-trigger[aria-expanded="true"] { color: #b5d5d2; border-color: #29404e; background: #101f2b; opacity: 1; }
  .atmosphere-trigger:focus-visible, .atmosphere-panel button:focus-visible { outline: 2px solid #78c7b7; outline-offset: 2px; }
  .atmosphere-panel { position: absolute; z-index: 100; top: 52px; right: 8px; width: 260px; padding: 15px; border: 1px solid #2a3d4d; border-radius: 8px; background: #0d1824f5; box-shadow: 0 14px 40px #02070deb; color: #e8eff5; font-size: 12px; backdrop-filter: blur(10px); }
  .setting-head { display: flex; justify-content: space-between; gap: 10px; align-items: baseline; margin-bottom: 12px; }
  .setting-head strong { font-size: 12px; font-weight: 600; }
  .setting-head span, .atmosphere-panel p { color: #8293a4; font-size: 10px; }
  .segmented, .season-grid { display: grid; gap: 4px; }
  .segmented { grid-template-columns: repeat(3, 1fr); }
  .season-grid { grid-template-columns: repeat(2, 1fr); }
  .segmented button, .season-grid button { min-height: 32px; border: 1px solid #26394a; border-radius: 5px; background: #101d2a; color: #9fafbd; font-size: 11px; }
  .segmented button.active, .season-grid button.active { border-color: #416d69; background: #14302f; color: #b5e7db; }
  .atmosphere-panel p { margin: 8px 1px 0; }
  .setting-label { margin: 13px 0 7px; color: #9fafbd; font-size: 10px; letter-spacing: .08em; text-transform: uppercase; }
  @media (min-width: 1600px) { .landscape { width: 60%; } }
  @media (max-width: 1150px) { .landscape { width: 80%; } }
  @media (max-width: 767px) {
    .photo-landscape { opacity: .62; }
    .photo-landscape img { inset: 0; width: 100%; height: 100%; object-position: 72% center; }
    .photo-landscape::before { background: linear-gradient(90deg, #07111cf5 0%, #07111cb8 52%, transparent 100%); }
    .landscape { width: 100%; right: -70px; opacity: .45; }
    .atmosphere-trigger { top: 8px; right: 0; width: 44px; height: 44px; }
    .atmosphere-panel { top: 52px; right: 0; width: min(260px, calc(100vw - 48px)); }
    .segmented button, .season-grid button { min-height: 44px; }
  }
  @media (prefers-reduced-motion: reduce) {
    .leaf, .snow { animation: none; }
    .air, .warning { transition: none; }
  }
</style>
