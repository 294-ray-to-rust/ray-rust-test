// Copyright 2025 The Ray Authors.
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

//! FallbackStrategy FFI bridge.
//!
//! This implements fallback strategy parsing and management
//! matching Ray's fallback_strategy.h

use crate::common::label_selector::{LabelConstraint, LabelSelectorOperator, RustLabelSelector};
use std::collections::HashSet;
use std::hash::{Hash, Hasher};

/// A single fallback option containing a label selector.
#[derive(Debug, Clone)]
pub struct RustFallbackOption {
    pub label_selector: RustLabelSelector,
}

impl RustFallbackOption {
    pub fn new(label_selector: RustLabelSelector) -> Self {
        Self { label_selector }
    }

    pub fn empty() -> Self {
        Self {
            label_selector: RustLabelSelector::new(),
        }
    }
}

impl PartialEq for RustFallbackOption {
    fn eq(&self, other: &Self) -> bool {
        self.label_selector == other.label_selector
    }
}

impl Eq for RustFallbackOption {}

impl Hash for RustFallbackOption {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.label_selector.hash(state);
    }
}

/// A fallback strategy containing multiple fallback options.
#[derive(Debug, Clone, Default)]
pub struct RustFallbackStrategy {
    options: Vec<RustFallbackOption>,
}

impl RustFallbackStrategy {
    pub fn new() -> Self {
        Self {
            options: Vec::new(),
        }
    }

    pub fn with_options(options: Vec<RustFallbackOption>) -> Self {
        Self { options }
    }

    pub fn add_option(&mut self, option: RustFallbackOption) {
        self.options.push(option);
    }

    pub fn options(&self) -> &[RustFallbackOption] {
        &self.options
    }

    pub fn len(&self) -> usize {
        self.options.len()
    }

    pub fn is_empty(&self) -> bool {
        self.options.is_empty()
    }

    /// Serialize to a list of string maps (simulating proto serialization).
    pub fn serialize(&self) -> Vec<Vec<(String, String)>> {
        self.options
            .iter()
            .map(|opt| {
                opt.label_selector
                    .to_string_map()
                    .into_iter()
                    .collect()
            })
            .collect()
    }

    /// Parse from a list of string maps (simulating proto deserialization).
    pub fn parse(data: Vec<Vec<(String, String)>>) -> Self {
        let options = data
            .into_iter()
            .map(|pairs| {
                let mut selector = RustLabelSelector::new();
                for (key, value) in pairs {
                    selector.add_constraint_str(&key, &value);
                }
                RustFallbackOption::new(selector)
            })
            .collect();
        Self { options }
    }
}

impl PartialEq for RustFallbackStrategy {
    fn eq(&self, other: &Self) -> bool {
        self.options == other.options
    }
}

impl Eq for RustFallbackStrategy {}

// ============================================================================
// FFI Bridge
// ============================================================================

#[cxx::bridge(namespace = "ray::ffi")]
mod ffi {
    /// Serialized option data.
    #[derive(Debug, Clone)]
    struct FallbackOptionData {
        /// Keys of the label selector constraints.
        keys: Vec<String>,
        /// Values of the label selector constraints.
        values: Vec<String>,
    }

    extern "Rust" {
        type RustFallbackOption;
        type RustFallbackStrategy;

        // FallbackOption
        fn fallback_option_new(
            selector_keys: &Vec<String>,
            selector_values: &Vec<String>,
        ) -> Box<RustFallbackOption>;
        fn fallback_option_empty() -> Box<RustFallbackOption>;
        fn fallback_option_equals(a: &RustFallbackOption, b: &RustFallbackOption) -> bool;
        fn fallback_option_hash(option: &RustFallbackOption) -> u64;
        fn fallback_option_get_selector_keys(option: &RustFallbackOption) -> Vec<String>;
        fn fallback_option_get_selector_values(option: &RustFallbackOption) -> Vec<String>;
        fn fallback_option_clone(option: &RustFallbackOption) -> Box<RustFallbackOption>;

        // FallbackStrategy
        fn fallback_strategy_new() -> Box<RustFallbackStrategy>;
        fn fallback_strategy_add_option(
            strategy: &mut RustFallbackStrategy,
            selector_keys: &Vec<String>,
            selector_values: &Vec<String>,
        );
        fn fallback_strategy_len(strategy: &RustFallbackStrategy) -> usize;
        fn fallback_strategy_is_empty(strategy: &RustFallbackStrategy) -> bool;
        fn fallback_strategy_get_option(
            strategy: &RustFallbackStrategy,
            index: usize,
        ) -> FallbackOptionData;
        fn fallback_strategy_equals(
            a: &RustFallbackStrategy,
            b: &RustFallbackStrategy,
        ) -> bool;

        // Serialization/parsing
        fn fallback_strategy_serialize(
            strategy: &RustFallbackStrategy,
        ) -> Vec<FallbackOptionData>;
        fn fallback_strategy_parse(data: &Vec<FallbackOptionData>) -> Box<RustFallbackStrategy>;
    }
}

use ffi::FallbackOptionData;
use std::collections::hash_map::DefaultHasher;

fn fallback_option_new(
    selector_keys: &Vec<String>,
    selector_values: &Vec<String>,
) -> Box<RustFallbackOption> {
    let mut selector = RustLabelSelector::new();
    for (key, value) in selector_keys.iter().zip(selector_values.iter()) {
        selector.add_constraint_str(key, value);
    }
    Box::new(RustFallbackOption::new(selector))
}

fn fallback_option_empty() -> Box<RustFallbackOption> {
    Box::new(RustFallbackOption::empty())
}

fn fallback_option_equals(a: &RustFallbackOption, b: &RustFallbackOption) -> bool {
    a == b
}

fn fallback_option_hash(option: &RustFallbackOption) -> u64 {
    let mut hasher = DefaultHasher::new();
    option.hash(&mut hasher);
    hasher.finish()
}

fn fallback_option_get_selector_keys(option: &RustFallbackOption) -> Vec<String> {
    let map = option.label_selector.to_string_map();
    let mut keys: Vec<_> = map.keys().cloned().collect();
    keys.sort();
    keys
}

fn fallback_option_get_selector_values(option: &RustFallbackOption) -> Vec<String> {
    let map = option.label_selector.to_string_map();
    let mut keys: Vec<_> = map.keys().cloned().collect();
    keys.sort();
    keys.iter().map(|k| map[k].clone()).collect()
}

fn fallback_option_clone(option: &RustFallbackOption) -> Box<RustFallbackOption> {
    Box::new(option.clone())
}

fn fallback_strategy_new() -> Box<RustFallbackStrategy> {
    Box::new(RustFallbackStrategy::new())
}

fn fallback_strategy_add_option(
    strategy: &mut RustFallbackStrategy,
    selector_keys: &Vec<String>,
    selector_values: &Vec<String>,
) {
    let mut selector = RustLabelSelector::new();
    for (key, value) in selector_keys.iter().zip(selector_values.iter()) {
        selector.add_constraint_str(key, value);
    }
    strategy.add_option(RustFallbackOption::new(selector));
}

fn fallback_strategy_len(strategy: &RustFallbackStrategy) -> usize {
    strategy.len()
}

fn fallback_strategy_is_empty(strategy: &RustFallbackStrategy) -> bool {
    strategy.is_empty()
}

fn fallback_strategy_get_option(
    strategy: &RustFallbackStrategy,
    index: usize,
) -> FallbackOptionData {
    let option = &strategy.options[index];
    let map = option.label_selector.to_string_map();
    let mut keys: Vec<_> = map.keys().cloned().collect();
    keys.sort();
    let values: Vec<_> = keys.iter().map(|k| map[k].clone()).collect();
    FallbackOptionData { keys, values }
}

fn fallback_strategy_equals(a: &RustFallbackStrategy, b: &RustFallbackStrategy) -> bool {
    a == b
}

fn fallback_strategy_serialize(strategy: &RustFallbackStrategy) -> Vec<FallbackOptionData> {
    strategy
        .options
        .iter()
        .map(|opt| {
            let map = opt.label_selector.to_string_map();
            let mut keys: Vec<_> = map.keys().cloned().collect();
            keys.sort();
            let values: Vec<_> = keys.iter().map(|k| map[k].clone()).collect();
            FallbackOptionData { keys, values }
        })
        .collect()
}

fn fallback_strategy_parse(data: &Vec<FallbackOptionData>) -> Box<RustFallbackStrategy> {
    let options = data
        .iter()
        .map(|d| {
            let mut selector = RustLabelSelector::new();
            for (key, value) in d.keys.iter().zip(d.values.iter()) {
                selector.add_constraint_str(key, value);
            }
            RustFallbackOption::new(selector)
        })
        .collect();
    Box::new(RustFallbackStrategy::with_options(options))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    #[test]
    fn test_fallback_option_construction_and_equality() {
        let mut map_a = HashMap::new();
        map_a.insert("region".to_string(), "us-east-1".to_string());
        let selector_a = RustLabelSelector::from_map(&map_a);

        let mut map_b = HashMap::new();
        map_b.insert("region".to_string(), "us-east-1".to_string());
        let selector_b = RustLabelSelector::from_map(&map_b);

        let mut map_c = HashMap::new();
        map_c.insert("region".to_string(), "us-west-2".to_string());
        let selector_c = RustLabelSelector::from_map(&map_c);

        let option_a = RustFallbackOption::new(selector_a);
        let option_b = RustFallbackOption::new(selector_b);
        let option_c = RustFallbackOption::new(selector_c);

        assert_eq!(option_a, option_b);
        assert_ne!(option_a, option_c);
    }

    #[test]
    fn test_fallback_option_hashing() {
        let mut map_a = HashMap::new();
        map_a.insert("key1".to_string(), "val1".to_string());
        let selector_a = RustLabelSelector::from_map(&map_a);

        let mut map_b = HashMap::new();
        map_b.insert("key1".to_string(), "val1".to_string());
        let selector_b = RustLabelSelector::from_map(&map_b);

        let mut map_c = HashMap::new();
        map_c.insert("key2".to_string(), "val2".to_string());
        let selector_c = RustLabelSelector::from_map(&map_c);

        let option_a = RustFallbackOption::new(selector_a);
        let option_b = RustFallbackOption::new(selector_b);
        let option_c = RustFallbackOption::new(selector_c);

        let mut hasher_a = DefaultHasher::new();
        option_a.hash(&mut hasher_a);
        let hash_a = hasher_a.finish();

        let mut hasher_b = DefaultHasher::new();
        option_b.hash(&mut hasher_b);
        let hash_b = hasher_b.finish();

        let mut hasher_c = DefaultHasher::new();
        option_c.hash(&mut hasher_c);
        let hash_c = hasher_c.finish();

        assert_eq!(hash_a, hash_b);
        assert_ne!(hash_a, hash_c);
    }

    #[test]
    fn test_fallback_strategy_serialize_and_parse() {
        let mut map1 = HashMap::new();
        map1.insert("region".to_string(), "us-east-1".to_string());
        map1.insert("market-type".to_string(), "spot".to_string());

        let mut map2 = HashMap::new();
        map2.insert("cpu-family".to_string(), "intel".to_string());

        let mut strategy = RustFallbackStrategy::new();
        strategy.add_option(RustFallbackOption::new(RustLabelSelector::from_map(&map1)));
        strategy.add_option(RustFallbackOption::new(RustLabelSelector::from_map(&map2)));

        assert_eq!(strategy.len(), 2);

        // Serialize and parse
        let serialized = strategy.serialize();
        let parsed = RustFallbackStrategy::parse(serialized);

        assert_eq!(strategy, parsed);
        assert_eq!(parsed.len(), 2);

        let parsed_map1 = parsed.options[0].label_selector.to_string_map();
        assert_eq!(parsed_map1.get("region"), Some(&"us-east-1".to_string()));
        assert_eq!(parsed_map1.get("market-type"), Some(&"spot".to_string()));

        let parsed_map2 = parsed.options[1].label_selector.to_string_map();
        assert_eq!(parsed_map2.get("cpu-family"), Some(&"intel".to_string()));
    }

    #[test]
    fn test_empty_fallback_strategy() {
        let strategy = RustFallbackStrategy::new();
        assert!(strategy.is_empty());
        assert_eq!(strategy.len(), 0);

        let serialized = strategy.serialize();
        assert!(serialized.is_empty());

        let parsed = RustFallbackStrategy::parse(serialized);
        assert!(parsed.is_empty());
    }
}
