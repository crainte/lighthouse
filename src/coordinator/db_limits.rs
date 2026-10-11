//! Container-aware DuckDB resource bounds. DuckDB sizes itself from host
//! totals, so inside a container with a smaller cgroup limit it overcommits
//! and gets OOM-killed. Unset config values are derived from the cgroup
//! (v2 first, then v1), falling back to host totals.

use crate::config::DatabaseConfig;
use anyhow::Result;
use std::path::Path;

/// Fraction of available memory handed to DuckDB; the rest is headroom for
/// the process itself (HTTP, caches, agent fan-out, allocator slack).
const MEMORY_FRACTION_PERCENT: u64 = 50;
const MIB: u64 = 1 << 20;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DatabaseLimits {
    pub memory_limit: String,
    pub threads: u32,
}

impl DatabaseLimits {
    pub fn resolve(config: &DatabaseConfig) -> Result<Self> {
        let cgroup = Path::new("/sys/fs/cgroup");
        let meminfo = std::fs::read_to_string("/proc/meminfo").unwrap_or_default();
        Ok(Self::derive(
            config.resolved_memory_limit(),
            config.resolved_threads()?,
            cgroup,
            &meminfo,
        ))
    }

    fn derive(memory: Option<String>, threads: Option<u32>, cgroup: &Path, meminfo: &str) -> Self {
        let memory_limit = memory.unwrap_or_else(|| {
            let available = match (cgroup_memory(cgroup), host_memory(meminfo)) {
                (Some(limit), Some(host)) => Some(limit.min(host)),
                (limit, host) => limit.or(host),
            };
            match available {
                Some(bytes) => format!(
                    "{}MiB",
                    (bytes / MIB * MEMORY_FRACTION_PERCENT / 100).max(64)
                ),
                None => "1GiB".to_string(),
            }
        });
        let threads = threads
            .or_else(|| cgroup_cpus(cgroup))
            .unwrap_or_else(|| std::thread::available_parallelism().map_or(1, |n| n.get() as u32))
            .max(1);
        Self {
            memory_limit,
            threads,
        }
    }
}

fn read(path: &Path) -> Option<String> {
    std::fs::read_to_string(path)
        .ok()
        .map(|s| s.trim().to_string())
}

/// v2 `memory.max` (`max` = unlimited), else v1 `memory.limit_in_bytes`,
/// whose "unlimited" is a near-`i64::MAX` value the host-total `min` absorbs.
fn cgroup_memory(root: &Path) -> Option<u64> {
    read(&root.join("memory.max"))
        .or_else(|| read(&root.join("memory/memory.limit_in_bytes")))
        .and_then(|value| value.parse().ok())
}

/// v2 `cpu.max` (`<quota> <period>` or `max <period>`), else v1
/// `cpu.cfs_quota_us` / `cpu.cfs_period_us` (`-1` = unlimited). Rounded up so
/// a 1.5-CPU quota gets two threads.
fn cgroup_cpus(root: &Path) -> Option<u32> {
    let (quota, period) = match read(&root.join("cpu.max")) {
        Some(line) => {
            let mut parts = line.split_whitespace();
            (parts.next()?.to_string(), parts.next()?.to_string())
        }
        None => (
            read(&root.join("cpu/cpu.cfs_quota_us"))?,
            read(&root.join("cpu/cpu.cfs_period_us"))?,
        ),
    };
    let quota: u64 = quota.parse().ok()?;
    let period: u64 = period.parse().ok().filter(|p| *p > 0)?;
    Some(quota.div_ceil(period).max(1) as u32)
}

fn host_memory(meminfo: &str) -> Option<u64> {
    meminfo
        .lines()
        .find_map(|line| line.strip_prefix("MemTotal:"))
        .and_then(|rest| rest.trim().strip_suffix("kB"))
        .and_then(|kib| kib.trim().parse::<u64>().ok())
        .map(|kib| kib * 1024)
}

#[cfg(test)]
mod tests {
    use super::*;

    const MEMINFO: &str = "MemTotal:       16384000 kB\nMemFree:  1 kB\n";

    fn fixture(files: &[(&str, &str)]) -> tempfile_dir::Dir {
        let dir = tempfile_dir::Dir::new();
        for (name, contents) in files {
            let path = dir.0.join(name);
            std::fs::create_dir_all(path.parent().unwrap()).unwrap();
            std::fs::write(path, contents).unwrap();
        }
        dir
    }

    mod tempfile_dir {
        pub struct Dir(pub std::path::PathBuf);
        impl Dir {
            pub fn new() -> Self {
                let path = std::env::temp_dir()
                    .join(format!("lighthouse-cgroup-{}", uuid::Uuid::now_v7()));
                std::fs::create_dir_all(&path).unwrap();
                Self(path)
            }
        }
        impl Drop for Dir {
            fn drop(&mut self) {
                let _ = std::fs::remove_dir_all(&self.0);
            }
        }
    }

    #[test]
    fn cgroup_v2_limits_bound_memory_and_threads() {
        let dir = fixture(&[
            ("memory.max", "2147483648\n"),
            ("cpu.max", "150000 100000\n"),
        ]);
        let limits = DatabaseLimits::derive(None, None, &dir.0, MEMINFO);
        assert_eq!(
            limits,
            DatabaseLimits {
                memory_limit: "1024MiB".into(),
                threads: 2
            }
        );
    }

    #[test]
    fn cgroup_v2_unlimited_falls_back_to_host_memory() {
        let dir = fixture(&[("memory.max", "max\n"), ("cpu.max", "max 100000\n")]);
        let limits = DatabaseLimits::derive(None, Some(3), &dir.0, MEMINFO);
        assert_eq!(limits.memory_limit, "8000MiB");
        assert_eq!(limits.threads, 3);
    }

    #[test]
    fn cgroup_v1_limits_and_unlimited_sentinel() {
        let dir = fixture(&[
            ("memory/memory.limit_in_bytes", "536870912\n"),
            ("cpu/cpu.cfs_quota_us", "400000\n"),
            ("cpu/cpu.cfs_period_us", "100000\n"),
        ]);
        let limits = DatabaseLimits::derive(None, None, &dir.0, MEMINFO);
        assert_eq!(
            limits,
            DatabaseLimits {
                memory_limit: "256MiB".into(),
                threads: 4
            }
        );

        let unlimited = fixture(&[
            ("memory/memory.limit_in_bytes", "9223372036854771712\n"),
            ("cpu/cpu.cfs_quota_us", "-1\n"),
            ("cpu/cpu.cfs_period_us", "100000\n"),
        ]);
        let limits = DatabaseLimits::derive(None, Some(1), &unlimited.0, MEMINFO);
        assert_eq!(limits.memory_limit, "8000MiB");
    }

    #[test]
    fn explicit_values_override_detection() {
        let dir = fixture(&[
            ("memory.max", "1073741824\n"),
            ("cpu.max", "100000 100000\n"),
        ]);
        let limits = DatabaseLimits::derive(Some("3GiB".into()), Some(6), &dir.0, MEMINFO);
        assert_eq!(
            limits,
            DatabaseLimits {
                memory_limit: "3GiB".into(),
                threads: 6
            }
        );
    }

    #[test]
    fn derived_memory_limit_is_accepted_by_duckdb() {
        let storage = crate::coordinator::storage::Storage::open(":memory:").unwrap();
        let effective = storage
            .apply_limits(&DatabaseLimits {
                memory_limit: "512MiB".into(),
                threads: 2,
            })
            .unwrap();
        assert_eq!(effective.threads, 2);
        assert!(effective.memory_limit.contains("MiB") || effective.memory_limit.contains("MB"));
    }
}
