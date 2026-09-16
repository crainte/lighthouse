//! HTTP surface, grouped by who is allowed to call it rather than by shape.
//!
//! `mod.rs` still owns the routes an admin or a public visitor reaches, and
//! the `router()` wiring for all of them; what lives under here is the
//! surface reserved for a coordinator's own peers. Splitting on caller
//! rather than on resource is what keeps a gate like `require_peer` next to
//! every handler it protects instead of scattered across a file organised
//! by endpoint shape.
pub(super) mod agents;
pub(super) mod checks;
pub(super) mod incidents;
pub(super) mod peer;
pub(super) mod policies;
pub(super) mod public;
pub(super) mod sections;
pub(super) mod session;
pub(super) mod settings;
