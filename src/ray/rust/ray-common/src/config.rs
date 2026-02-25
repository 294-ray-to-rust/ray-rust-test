// Copyright 2017 The Ray Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Configuration types for Ray.
//!
//! This module provides a Rust equivalent of Ray's C++ RayConfig.

use serde::{Deserialize, Serialize};

/// Ray configuration values.
///
/// This struct contains key configuration values used throughout Ray.
/// It mirrors a subset of the C++ RayConfig class.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RayConfig {
    /// Whether task events are enabled by default.
    pub task_events_enabled: bool,

    /// The precision of fractional resource quantity.
    pub resource_unit_scaling: i32,

    /// Length of Ray full-length IDs in bytes.
    pub unique_id_size: usize,

    /// Object ID index size in bits.
    pub object_id_index_size: i32,

    /// Ray version string.
    pub ray_version: String,
}

impl Default for RayConfig {
    fn default() -> Self {
        Self {
            task_events_enabled: true,
            resource_unit_scaling: 10000,
            unique_id_size: 28,
            object_id_index_size: 32,
            ray_version: "3.0.0.dev0".to_string(),
        }
    }
}

impl RayConfig {
    /// Create a new RayConfig with default values.
    pub fn new() -> Self {
        Self::default()
    }

    /// Get the global Ray configuration.
    ///
    /// In a real implementation, this would read from environment
    /// variables or configuration files.
    pub fn instance() -> &'static RayConfig {
        static INSTANCE: std::sync::OnceLock<RayConfig> = std::sync::OnceLock::new();
        INSTANCE.get_or_init(RayConfig::default)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_config() {
        let config = RayConfig::default();
        assert!(config.task_events_enabled);
        assert_eq!(config.resource_unit_scaling, 10000);
        assert_eq!(config.unique_id_size, 28);
        assert_eq!(config.object_id_index_size, 32);
    }

    #[test]
    fn test_singleton_instance() {
        let config1 = RayConfig::instance();
        let config2 = RayConfig::instance();
        assert_eq!(config1.ray_version, config2.ray_version);
    }
}
