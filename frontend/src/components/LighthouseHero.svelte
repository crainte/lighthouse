<script lang="ts">
  import { onMount } from 'svelte';

  export type LighthouseHealth = 'operational' | 'degraded' | 'down' | 'unknown';

  let { health = 'operational' }: { health?: LighthouseHealth } = $props();

  let recovering = $state(false);
  let previousHealth: LighthouseHealth | null = null;
  let recoveryTimer: ReturnType<typeof setTimeout> | undefined;

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

  const SWEEP_KEY = 'lightkeeper-lighthouse-sweep-v1';
  let sweptInMemory = false;
  let sweeping = $state(false);

  onMount(() => {
    const motion = window.matchMedia('(prefers-reduced-motion: reduce)');
    let timer: ReturnType<typeof window.setTimeout> | undefined;

    const hasSwept = () => {
      if (sweptInMemory) return true;
      try {
        return sessionStorage.getItem(SWEEP_KEY) === 'seen';
      } catch {
        return false;
      }
    };

    const settle = () => {
      if (timer !== undefined) window.clearTimeout(timer);
      timer = undefined;
      sweeping = false;
    };

    const start = () => {
      if (document.visibilityState !== 'visible' || motion.matches || hasSwept()) return;
      sweptInMemory = true;
      try {
        sessionStorage.setItem(SWEEP_KEY, 'seen');
      } catch {
      }
      sweeping = true;
      timer = window.setTimeout(settle, 2100);
    };

    const handleVisibility = () => {
      if (document.visibilityState === 'hidden') settle();
      else start();
    };
    const handleMotion = () => {
      if (motion.matches) settle();
      else start();
    };

    document.addEventListener('visibilitychange', handleVisibility);
    motion.addEventListener('change', handleMotion);
    start();

    return () => {
      settle();
      document.removeEventListener('visibilitychange', handleVisibility);
      motion.removeEventListener('change', handleMotion);
    };
  });
</script>

<svg class="landscape {health}" class:recovering viewBox="0 0 800 250" preserveAspectRatio="xMidYMid slice" aria-hidden="true" focusable="false">
    <defs>
      <linearGradient id="lighthouse-sky" x2="0" y2="1"><stop stop-color="#0a1625"/><stop offset=".67" stop-color="#112b36"/><stop offset="1" stop-color="#080f19"/></linearGradient>
      <radialGradient id="lighthouse-air"><stop stop-color="#49878b" stop-opacity=".16"/><stop offset="1" stop-color="#234451" stop-opacity="0"/></radialGradient>
      <linearGradient id="lighthouse-sea" x2="0" y2="1"><stop stop-color="#1a3741" stop-opacity=".6"/><stop offset="1" stop-color="#080f19"/></linearGradient>
      <linearGradient id="lighthouse-stone"><stop stop-color="#233e49"/><stop offset=".45" stop-color="#6c7b7b"/><stop offset=".6" stop-color="#344d57"/><stop offset="1" stop-color="#152a35"/></linearGradient>
      <linearGradient id="lighthouse-beam"><stop stop-color="#f0dfb4" stop-opacity=".34"/><stop offset="1" stop-color="#a4d8d0" stop-opacity="0"/></linearGradient>
      <radialGradient id="lighthouse-glow"><stop stop-color="#fff0c9" stop-opacity=".8"/><stop offset=".15" stop-color="#ecd9a9" stop-opacity=".3"/><stop offset="1" stop-color="#e7c98a" stop-opacity="0"/></radialGradient>
      <radialGradient id="lighthouse-amber"><stop stop-color="#e7b66b" stop-opacity=".2"/><stop offset="1" stop-color="#8a5f2a" stop-opacity="0"/></radialGradient>
      <radialGradient id="lighthouse-storm"><stop stop-color="#ee8389" stop-opacity=".13"/><stop offset="1" stop-color="#5b2733" stop-opacity="0"/></radialGradient>
      <radialGradient id="lighthouse-warning"><stop stop-color="#ffd0cf" stop-opacity=".95"/><stop offset=".18" stop-color="#ee8389" stop-opacity=".7"/><stop offset="1" stop-color="#ee8389" stop-opacity="0"/></radialGradient>
      <linearGradient id="lighthouse-fog"><stop stop-color="#74a8aa" stop-opacity="0"/><stop offset=".6" stop-color="#74a8aa" stop-opacity=".12"/><stop offset="1" stop-color="#74a8aa" stop-opacity="0"/></linearGradient>
    </defs>
    <path fill="url(#lighthouse-sky)" d="M0 0h800v250H0z"/><ellipse class="air calm-air" cx="578" cy="140" rx="280" ry="180" fill="url(#lighthouse-air)"/>
    <ellipse class="air amber-air" cx="590" cy="120" rx="300" ry="170" fill="url(#lighthouse-amber)"/>
    <ellipse class="air storm-air" cx="590" cy="125" rx="320" ry="175" fill="url(#lighthouse-storm)"/>
    <path d="M0 169Q160 165 300 168T800 168V250H0Z" fill="url(#lighthouse-sea)"/><path d="M140 168h660" stroke="#678e94" opacity=".12"/>
    <path d="m30 172 96-8 80 4 70-9 60 10 90-2 64 7Z" fill="#0c1c28"/>
    <path d="M590 83 805 44v55Z" fill="url(#lighthouse-beam)" opacity=".42"/>
    <g class:sweeping class="beam">
      <path class="beam-right-outer" d="M590 83 1050-5v185Z" fill="#d9eee0" opacity=".28"/><path class="beam-right-core" d="M590 83 1050 48v70Z" fill="#fff0c9" opacity=".2"/>
      <path class="beam-left" d="M590 83 160 12v142Z" fill="#b3d8d5" opacity=".1"/>
    </g>
    <circle class="lantern-aura" class:sweeping cx="590" cy="83" r="53" fill="url(#lighthouse-glow)"/>
    <path d="m450 232 29-27 38-10 25-15 45-4 44 11 24-3 47 20 26 13 52 14 20 19H420Z" fill="#0b1520"/>
    <path d="m472 217 58-23 25-8 33-2 35 13-30-6-35 8-21-1-23 12Z" fill="#1d3039"/><path d="m616 199 24-6 27 12 31 20-40-13-26 1Z" fill="#152632"/>
    <path d="m568 189 10-89h24l12 91-21 7Z" fill="url(#lighthouse-stone)"/><path d="m580 102-6 86 11 4 2-90Z" fill="#a3aca0" opacity=".09"/>
    <path d="M575 98h31v5h-31Z" fill="#273c43"/><path d="M573 94h35v4h-35Z" fill="#8e9690" opacity=".6"/>
    <path d="M578 72h25v20h-25Z" fill="#112330"/><path d="M581 76h19v13h-19Z" fill="#d7c995" opacity=".56"/><path d="M584 77h12v10h-12Z" fill="#fff0c2"/>
    <path d="M581 74v17m9-17v17m9-17v17" stroke="#304147" stroke-width="2"/><path d="M575 72h31l-16-10Z" fill="#547078"/><path d="M575 72h31" stroke="#8c9d99"/>
    <path d="M590 57v5M571 91h39M572 88v7m6-7v7m24-7v7m7-7v7" stroke="#627c7c" stroke-width="1"/>
    <g class="warning"><circle cx="590" cy="56" r="16" fill="url(#lighthouse-warning)"/><circle cx="590" cy="56" r="1.7" fill="#ffd9d7"/></g>
    <path d="M587 116h5v9h-5Zm-2 25h6v9h-6Zm-2 27h7v20h-7Z" fill="#0d202b"/><path d="M588 117h2v6h-2Z" fill="#e3ca8d" opacity=".45"/>
    <path d="M550 191h78v5h-78Z" fill="#2a3c42"/>
    <path d="M320 192q180-19 400 3l80 12q-220-17-480-1Z" fill="url(#lighthouse-fog)"/><path d="M220 217q200-15 480 11v10q-260-17-480-12Z" fill="url(#lighthouse-fog)"/>
    <circle cx="344" cy="167" r="1.3" fill="#d1c193" opacity=".7"/><path d="M339 174h12m-16 5h19M712 180h37m-12 7h33M389 185h44" stroke="#9dbdb9" opacity=".09"/>
  </svg>

<style>
  .landscape { position: absolute; inset: 0 0 0 auto; display: block; width: 65%; height: 100%; z-index: 0; mask-image: linear-gradient(90deg, transparent, #000 24%); pointer-events: none; }
  .lantern-aura { opacity: .5; transition: opacity 1.6s ease; }
  .air, .warning { transition: opacity 1.6s ease; }
  .amber-air, .storm-air, .warning { opacity: 0; }
  .degraded .calm-air { opacity: .55; }
  .degraded .amber-air { opacity: 1; }
  .down .calm-air { opacity: .45; }
  .down .amber-air { opacity: .35; }
  .down .storm-air, .down .warning { opacity: 1; }
  .unknown .calm-air { opacity: .5; }
  .unknown .lantern-aura { opacity: .38; }
  .recovering .lantern-aura { animation: recover 2.6s ease-out; }
  @keyframes recover { 0%, 100% { opacity: .5; } 35% { opacity: .9; } }
  .lantern-aura.sweeping { animation: lantern 2.1s ease-out; }
  @keyframes lantern { 0%, 100% { opacity: .5; } 40% { opacity: 1; } }
  @media (min-width: 1600px) { .landscape { width: 60%; } }
  @media (max-width: 1150px) { .landscape { width: 80%; } }
  .beam path { transform-box: view-box; transform-origin: 590px 83px; opacity: 0; }
  .beam.sweeping .beam-right-outer { animation: beam-right 2.1s cubic-bezier(.37, .05, .25, 1) both; }
  .beam.sweeping .beam-right-core { animation: beam-right-core 2.1s cubic-bezier(.37, .05, .25, 1) both; }
  .beam.sweeping .beam-left { animation: beam-left 2.1s cubic-bezier(.37, .05, .25, 1) both; }
  @keyframes beam-right { 0%, 100% { opacity: 0; transform: scaleX(.08); } 20%, 62% { opacity: .28; } 50% { transform: scaleX(1); } }
  @keyframes beam-right-core { 0%, 100% { opacity: 0; transform: scaleX(.06); } 26%, 58% { opacity: .2; } 50% { transform: scaleX(1); } }
  @keyframes beam-left { 0%, 100% { opacity: 0; transform: scaleX(.08); } 45%, 82% { opacity: .1; } 66% { transform: scaleX(1); } }
  @media (max-width: 767px) { .landscape { width: 100%; right: -70px; opacity: .45; } }
  @media (prefers-reduced-motion: reduce) { .beam path, .lantern-aura { animation: none; } .air, .warning, .lantern-aura { transition: none; } }
</style>
