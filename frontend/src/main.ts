import './app.css';
import { mount } from 'svelte';
import AppRoot from './AppRoot.svelte';

const app = mount(AppRoot, { target: document.getElementById('app')! });

export default app;
