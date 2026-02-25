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

//! Shared test utilities for Ray Rust crates.

use ray_common::id::{JobId, RayId, TaskId};

/// Create a test JobId.
pub fn test_job_id(n: u32) -> JobId {
    JobId::from_int(n)
}

/// Create a random TaskId for testing.
pub fn test_task_id() -> TaskId {
    TaskId::from_random(&test_job_id(1))
}

/// Assert that two IDs have the same hex representation.
pub fn assert_hex_eq<T: RayId, U: RayId>(a: &T, b: &U) {
    assert_eq!(a.to_hex(), b.to_hex());
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_test_job_id() {
        let id = test_job_id(42);
        assert_eq!(id.to_int(), 42);
    }
}
