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

//! LabelSelector FFI bridge.
//!
//! This implements label selector parsing and constraint management
//! matching Ray's label_selector.h

use std::collections::{HashMap, HashSet};

/// Label selector operator enum matching Ray's LabelSelectorOperator.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum LabelSelectorOperator {
    Unspecified = 0,
    In = 1,
    NotIn = 2,
}

impl From<i32> for LabelSelectorOperator {
    fn from(v: i32) -> Self {
        match v {
            1 => LabelSelectorOperator::In,
            2 => LabelSelectorOperator::NotIn,
            _ => LabelSelectorOperator::Unspecified,
        }
    }
}

/// A label constraint with key, operator, and values.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LabelConstraint {
    key: String,
    operator: LabelSelectorOperator,
    values: HashSet<String>,
}

impl LabelConstraint {
    pub fn new(key: String, operator: LabelSelectorOperator, values: HashSet<String>) -> Self {
        Self {
            key,
            operator,
            values,
        }
    }

    pub fn key(&self) -> &str {
        &self.key
    }

    pub fn operator(&self) -> LabelSelectorOperator {
        self.operator
    }

    pub fn values(&self) -> &HashSet<String> {
        &self.values
    }

    /// Get values as a sorted vector (for deterministic output).
    pub fn sorted_values(&self) -> Vec<String> {
        let mut v: Vec<String> = self.values.iter().cloned().collect();
        v.sort();
        v
    }
}

/// Label selector containing multiple constraints.
#[derive(Debug, Clone, Default)]
pub struct RustLabelSelector {
    constraints: Vec<LabelConstraint>,
}

impl RustLabelSelector {
    pub fn new() -> Self {
        Self {
            constraints: Vec::new(),
        }
    }

    /// Create from a map of key-value pairs (like Python dict).
    pub fn from_map(map: &HashMap<String, String>) -> Self {
        let mut selector = Self::new();
        for (key, value) in map {
            selector.add_constraint_str(key, value);
        }
        selector
    }

    /// Parse a value string and add as constraint.
    /// Supports formats: "value", "in(a,b,c)", "!value", "!in(a,b,c)"
    pub fn add_constraint_str(&mut self, key: &str, value: &str) {
        let (op, values) = Self::parse_value(value);
        let constraint = LabelConstraint::new(key.to_string(), op, values);
        self.add_constraint(constraint);
    }

    /// Add a constraint, avoiding duplicates.
    pub fn add_constraint(&mut self, constraint: LabelConstraint) {
        // Check for duplicates
        if self.constraints.contains(&constraint) {
            return;
        }
        self.constraints.push(constraint);
    }

    /// Get all constraints.
    pub fn constraints(&self) -> &[LabelConstraint] {
        &self.constraints
    }

    /// Convert back to a string map.
    pub fn to_string_map(&self) -> HashMap<String, String> {
        let mut map = HashMap::new();

        for constraint in &self.constraints {
            let sorted_values = constraint.sorted_values();
            let value_str = match constraint.operator {
                LabelSelectorOperator::In => {
                    if sorted_values.len() == 1 {
                        sorted_values[0].clone()
                    } else {
                        format!("in({})", sorted_values.join(","))
                    }
                }
                LabelSelectorOperator::NotIn => {
                    if sorted_values.len() == 1 {
                        format!("!{}", sorted_values[0])
                    } else {
                        format!("!in({})", sorted_values.join(","))
                    }
                }
                LabelSelectorOperator::Unspecified => continue,
            };

            map.insert(constraint.key.clone(), value_str);
        }

        map
    }

    /// Parse a value string into operator and values.
    fn parse_value(value: &str) -> (LabelSelectorOperator, HashSet<String>) {
        let mut val = value;
        let mut is_negated = false;

        // Check for negation prefix
        if val.starts_with('!') {
            is_negated = true;
            val = &val[1..];
        }

        let mut values = HashSet::new();

        // Check for in() syntax
        if val.starts_with("in(") && val.ends_with(')') {
            // Remove "in(" prefix and ")" suffix
            let inner = &val[3..val.len() - 1];

            // Parse comma-separated values
            for token in inner.split(',') {
                if !token.is_empty() {
                    values.insert(token.to_string());
                }
            }
        } else {
            // Single value
            values.insert(val.to_string());
        }

        let op = if is_negated {
            LabelSelectorOperator::NotIn
        } else {
            LabelSelectorOperator::In
        };

        (op, values)
    }

    /// Debug string representation.
    pub fn debug_string(&self) -> String {
        let mut parts = Vec::new();
        for constraint in &self.constraints {
            let op_str = match constraint.operator {
                LabelSelectorOperator::In => "in",
                LabelSelectorOperator::NotIn => "!in",
                LabelSelectorOperator::Unspecified => "?",
            };
            let values: Vec<_> = constraint.values.iter().map(|s| format!("'{}'", s)).collect();
            parts.push(format!(
                "'{}': {} ({})",
                constraint.key,
                op_str,
                values.join(", ")
            ));
        }
        format!("{{{}}}", parts.join(", "))
    }
}

impl PartialEq for RustLabelSelector {
    fn eq(&self, other: &Self) -> bool {
        self.constraints == other.constraints
    }
}

impl Eq for RustLabelSelector {}

impl std::hash::Hash for RustLabelSelector {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.constraints.len().hash(state);
        for constraint in &self.constraints {
            constraint.key.hash(state);
            (constraint.operator as i32).hash(state);
            // Hash values in sorted order for consistency
            for val in constraint.sorted_values() {
                val.hash(state);
            }
        }
    }
}

impl std::hash::Hash for LabelConstraint {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.key.hash(state);
        (self.operator as i32).hash(state);
        // Hash values in sorted order for consistency
        let mut sorted: Vec<_> = self.values.iter().collect();
        sorted.sort();
        for val in sorted {
            val.hash(state);
        }
    }
}

// ============================================================================
// FFI Bridge
// ============================================================================

#[cxx::bridge(namespace = "ray::ffi")]
mod ffi {
    /// Operator enum for FFI.
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    #[repr(i32)]
    enum LabelOperator {
        Unspecified = 0,
        In = 1,
        NotIn = 2,
    }

    /// Constraint data returned from Rust.
    #[derive(Debug, Clone)]
    struct ConstraintData {
        key: String,
        op: LabelOperator,
        values: Vec<String>,
    }

    extern "Rust" {
        type RustLabelSelector;

        // Construction
        fn label_selector_new() -> Box<RustLabelSelector>;
        fn label_selector_from_map(
            keys: &Vec<String>,
            values: &Vec<String>,
        ) -> Box<RustLabelSelector>;

        // Adding constraints
        fn label_selector_add_constraint_str(
            selector: &mut RustLabelSelector,
            key: &str,
            value: &str,
        );
        fn label_selector_add_constraint(
            selector: &mut RustLabelSelector,
            key: &str,
            op: LabelOperator,
            values: &Vec<String>,
        );

        // Querying
        fn label_selector_num_constraints(selector: &RustLabelSelector) -> usize;
        fn label_selector_get_constraint(
            selector: &RustLabelSelector,
            index: usize,
        ) -> ConstraintData;
        fn label_selector_get_all_constraints(selector: &RustLabelSelector) -> Vec<ConstraintData>;

        // Conversion
        fn label_selector_to_string_map_keys(selector: &RustLabelSelector) -> Vec<String>;
        fn label_selector_to_string_map_values(selector: &RustLabelSelector) -> Vec<String>;
        fn label_selector_debug_string(selector: &RustLabelSelector) -> String;

        // Equality and cloning
        fn label_selector_clone(selector: &RustLabelSelector) -> Box<RustLabelSelector>;
        fn label_selector_equals(a: &RustLabelSelector, b: &RustLabelSelector) -> bool;
    }
}

use ffi::{ConstraintData, LabelOperator};

fn label_selector_new() -> Box<RustLabelSelector> {
    Box::new(RustLabelSelector::new())
}

fn label_selector_from_map(keys: &Vec<String>, values: &Vec<String>) -> Box<RustLabelSelector> {
    let map: HashMap<String, String> = keys
        .iter()
        .cloned()
        .zip(values.iter().cloned())
        .collect();
    Box::new(RustLabelSelector::from_map(&map))
}

fn label_selector_add_constraint_str(selector: &mut RustLabelSelector, key: &str, value: &str) {
    selector.add_constraint_str(key, value);
}

fn label_selector_add_constraint(
    selector: &mut RustLabelSelector,
    key: &str,
    op: LabelOperator,
    values: &Vec<String>,
) {
    let label_op = match op {
        LabelOperator::In => LabelSelectorOperator::In,
        LabelOperator::NotIn => LabelSelectorOperator::NotIn,
        _ => LabelSelectorOperator::Unspecified,
    };
    let value_set: HashSet<String> = values.iter().cloned().collect();
    let constraint = LabelConstraint::new(key.to_string(), label_op, value_set);
    selector.add_constraint(constraint);
}

fn label_selector_num_constraints(selector: &RustLabelSelector) -> usize {
    selector.constraints.len()
}

fn label_selector_get_constraint(selector: &RustLabelSelector, index: usize) -> ConstraintData {
    let constraint = &selector.constraints[index];
    ConstraintData {
        key: constraint.key.clone(),
        op: match constraint.operator {
            LabelSelectorOperator::In => LabelOperator::In,
            LabelSelectorOperator::NotIn => LabelOperator::NotIn,
            LabelSelectorOperator::Unspecified => LabelOperator::Unspecified,
        },
        values: constraint.sorted_values(),
    }
}

fn label_selector_get_all_constraints(selector: &RustLabelSelector) -> Vec<ConstraintData> {
    selector
        .constraints
        .iter()
        .map(|c| ConstraintData {
            key: c.key.clone(),
            op: match c.operator {
                LabelSelectorOperator::In => LabelOperator::In,
                LabelSelectorOperator::NotIn => LabelOperator::NotIn,
                LabelSelectorOperator::Unspecified => LabelOperator::Unspecified,
            },
            values: c.sorted_values(),
        })
        .collect()
}

fn label_selector_to_string_map_keys(selector: &RustLabelSelector) -> Vec<String> {
    let map = selector.to_string_map();
    // Sort keys for deterministic order
    let mut keys: Vec<_> = map.keys().cloned().collect();
    keys.sort();
    keys
}

fn label_selector_to_string_map_values(selector: &RustLabelSelector) -> Vec<String> {
    let map = selector.to_string_map();
    // Return values in the same sorted order as keys
    let mut keys: Vec<_> = map.keys().cloned().collect();
    keys.sort();
    keys.iter().map(|k| map[k].clone()).collect()
}

fn label_selector_debug_string(selector: &RustLabelSelector) -> String {
    selector.debug_string()
}

fn label_selector_clone(selector: &RustLabelSelector) -> Box<RustLabelSelector> {
    Box::new(selector.clone())
}

fn label_selector_equals(a: &RustLabelSelector, b: &RustLabelSelector) -> bool {
    a == b
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_basic_construction() {
        let mut map = HashMap::new();
        map.insert("market-type".to_string(), "spot".to_string());
        map.insert("region".to_string(), "us-east".to_string());

        let selector = RustLabelSelector::from_map(&map);
        assert_eq!(selector.constraints.len(), 2);

        for constraint in selector.constraints() {
            assert!(map.contains_key(constraint.key()));
            assert_eq!(constraint.operator(), LabelSelectorOperator::In);
            assert_eq!(constraint.values().len(), 1);
            assert!(constraint.values().contains(&map[constraint.key()]));
        }
    }

    #[test]
    fn test_in_operator_parsing() {
        let mut selector = RustLabelSelector::new();
        selector.add_constraint_str("region", "in(us-west,us-east,me-central)");

        assert_eq!(selector.constraints.len(), 1);
        let constraint = &selector.constraints[0];

        assert_eq!(constraint.operator(), LabelSelectorOperator::In);
        assert_eq!(constraint.values().len(), 3);
        assert!(constraint.values().contains("us-west"));
        assert!(constraint.values().contains("us-east"));
        assert!(constraint.values().contains("me-central"));
    }

    #[test]
    fn test_not_in_operator_parsing() {
        let mut selector = RustLabelSelector::new();
        selector.add_constraint_str("tier", "!in(premium,free)");

        assert_eq!(selector.constraints.len(), 1);
        let constraint = &selector.constraints[0];

        assert_eq!(constraint.operator(), LabelSelectorOperator::NotIn);
        assert_eq!(constraint.values().len(), 2);
        assert!(constraint.values().contains("premium"));
        assert!(constraint.values().contains("free"));
    }

    #[test]
    fn test_single_value_not_in() {
        let mut selector = RustLabelSelector::new();
        selector.add_constraint_str("env", "!dev");

        assert_eq!(selector.constraints.len(), 1);
        let constraint = &selector.constraints[0];

        assert_eq!(constraint.operator(), LabelSelectorOperator::NotIn);
        assert_eq!(constraint.values().len(), 1);
        assert!(constraint.values().contains("dev"));
    }

    #[test]
    fn test_to_string_map() {
        let mut selector = RustLabelSelector::new();

        // Single value IN
        selector.add_constraint(LabelConstraint::new(
            "region".to_string(),
            LabelSelectorOperator::In,
            ["us-west".to_string()].into_iter().collect(),
        ));

        // Multiple values IN
        selector.add_constraint(LabelConstraint::new(
            "tier".to_string(),
            LabelSelectorOperator::In,
            ["prod", "dev", "staging"]
                .iter()
                .map(|s| s.to_string())
                .collect(),
        ));

        // Single value NOT IN
        selector.add_constraint(LabelConstraint::new(
            "env".to_string(),
            LabelSelectorOperator::NotIn,
            ["dev".to_string()].into_iter().collect(),
        ));

        // Multiple values NOT IN
        selector.add_constraint(LabelConstraint::new(
            "team".to_string(),
            LabelSelectorOperator::NotIn,
            ["A100", "B200"].iter().map(|s| s.to_string()).collect(),
        ));

        let string_map = selector.to_string_map();

        assert_eq!(string_map.len(), 4);
        assert_eq!(string_map.get("region"), Some(&"us-west".to_string()));
        assert_eq!(string_map.get("env"), Some(&"!dev".to_string()));
        assert_eq!(
            string_map.get("tier"),
            Some(&"in(dev,prod,staging)".to_string())
        );
        assert_eq!(string_map.get("team"), Some(&"!in(A100,B200)".to_string()));
    }

    #[test]
    fn test_deduplication() {
        let mut selector = RustLabelSelector::new();

        selector.add_constraint_str("region", "us-west");
        assert_eq!(selector.constraints.len(), 1);

        // Add same constraint again - should not duplicate
        selector.add_constraint_str("region", "us-west");
        assert_eq!(selector.constraints.len(), 1);

        // Different value - should add
        selector.add_constraint_str("region", "us-east");
        assert_eq!(selector.constraints.len(), 2);

        // Different key - should add
        selector.add_constraint_str("location", "us-east");
        assert_eq!(selector.constraints.len(), 3);

        // Different key and value - should add
        selector.add_constraint_str("instance", "spot");
        assert_eq!(selector.constraints.len(), 4);

        // Duplicate via direct add - should not duplicate
        selector.add_constraint(LabelConstraint::new(
            "instance".to_string(),
            LabelSelectorOperator::In,
            ["spot".to_string()].into_iter().collect(),
        ));
        assert_eq!(selector.constraints.len(), 4);
    }
}
