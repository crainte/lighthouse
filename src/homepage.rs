//! Presentation-only spacers anchored after a loose check or a section.
//! Kept as one replicated settings document; neither check nor section order is rewritten.
use serde::{Deserialize, Serialize};
use std::collections::HashSet;

pub const KEY: &str = "homepage_spacers";

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct Spacer {
    pub id: String,
    pub after: Option<String>,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq)]
pub struct Spacers {
    pub spacers: Vec<Spacer>,
}

impl Spacers {
    pub fn validate(&self) -> Result<(), &'static str> {
        if self.spacers.len() > 100 {
            return Err("too many spacers");
        }
        let mut ids = HashSet::new();
        for spacer in &self.spacers {
            if spacer.id.is_empty() || spacer.id.len() > 100 || !ids.insert(&spacer.id) {
                return Err("spacer ids must be unique and between 1 and 100 bytes");
            }
            if let Some(after) = &spacer.after {
                if after.len() > 200
                    || !["check:", "section:"].iter().any(|prefix| {
                        after
                            .strip_prefix(prefix)
                            .is_some_and(|value| !value.is_empty())
                    })
                {
                    return Err("spacer anchor must be a check or section");
                }
            }
        }
        Ok(())
    }
}
