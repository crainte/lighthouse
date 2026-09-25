// Authenticated admin API, split by domain. This barrel re-exports everything
// so existing `from '../lib/admin'` / `from './lib/admin'` imports keep
// working unchanged; new code may import a domain module directly.

export type { ProbeStatus } from "../api";

export * from "./client";
export * from "./session";
export * from "./checks";
export * from "./sections";
export * from "./incidents";
export * from "./data";
export * from "./probe-results";
export * from "./agents";
export * from "./notifiers";
export * from "./maintenance";
export * from "./cluster";
