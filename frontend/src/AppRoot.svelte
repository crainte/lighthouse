<script lang="ts">
  import App from './App.svelte';
  import AppBoundary from './components/AppBoundary.svelte';
</script>

<!-- Boundary lives outside App so a failure in App itself is still caught. -->
<AppBoundary>
  <App />
</AppBoundary>
