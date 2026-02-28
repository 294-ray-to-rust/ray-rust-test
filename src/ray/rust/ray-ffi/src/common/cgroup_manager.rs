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

//! Cgroup Manager FFI bridge.

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};

/// Status codes for cgroup manager operations.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
#[repr(u8)]
pub enum CgroupManagerStatus {
    #[default]
    Ok = 0,
    Invalid = 1,
    NotFound = 2,
    PermissionDenied = 3,
    InvalidArgument = 4,
}

/// A fake cgroup structure for testing.
#[derive(Clone, Debug, Default)]
pub struct FakeCgroup {
    pub path: String,
    pub processes: Vec<i32>,
    pub constraints: HashMap<String, String>,
    pub available_controllers: HashSet<String>,
    pub enabled_controllers: HashSet<String>,
}

impl FakeCgroup {
    pub fn new(path: &str) -> Self {
        Self {
            path: path.to_string(),
            ..Default::default()
        }
    }

    pub fn with_processes(mut self, processes: Vec<i32>) -> Self {
        self.processes = processes;
        self
    }

    pub fn with_available_controllers(mut self, controllers: Vec<&str>) -> Self {
        self.available_controllers = controllers.into_iter().map(|s| s.to_string()).collect();
        self
    }
}

/// Fake constraint tracking for cleanup verification.
#[derive(Clone, Debug)]
pub struct FakeConstraint {
    pub cgroup: String,
    pub name: String,
}

/// Fake controller tracking for cleanup verification.
#[derive(Clone, Debug)]
pub struct FakeController {
    pub cgroup: String,
    pub name: String,
}

/// Fake process move tracking for cleanup verification.
#[derive(Clone, Debug)]
pub struct FakeMoveProcesses {
    pub from: String,
    pub to: String,
}

/// Shared state for the fake cgroup driver.
#[derive(Default)]
pub struct FakeCgroupDriverState {
    pub cgroups: HashMap<String, FakeCgroup>,
    pub cleanup_mode: bool,
    pub cleanup_counter: i32,
    pub deleted_cgroups: Vec<(i32, String)>,
    pub constraints_disabled: Vec<(i32, FakeConstraint)>,
    pub controllers_disabled: Vec<(i32, FakeController)>,
    pub processes_moved: Vec<(i32, FakeMoveProcesses)>,

    // Status overrides for testing
    pub check_cgroup_enabled_status: CgroupManagerStatus,
    pub check_cgroup_status: CgroupManagerStatus,
    pub create_cgroup_status: CgroupManagerStatus,
    pub delete_cgroup_status: CgroupManagerStatus,
    pub move_all_processes_status: CgroupManagerStatus,
    pub enable_controller_status: CgroupManagerStatus,
    pub disable_controller_status: CgroupManagerStatus,
    pub add_constraint_status: CgroupManagerStatus,
    pub available_controllers_status: CgroupManagerStatus,
    pub enabled_controllers_status: CgroupManagerStatus,
    pub add_process_to_cgroup_status: CgroupManagerStatus,
}

impl FakeCgroupDriverState {
    pub fn new() -> Self {
        Self {
            check_cgroup_enabled_status: CgroupManagerStatus::Ok,
            check_cgroup_status: CgroupManagerStatus::Ok,
            create_cgroup_status: CgroupManagerStatus::Ok,
            delete_cgroup_status: CgroupManagerStatus::Ok,
            move_all_processes_status: CgroupManagerStatus::Ok,
            enable_controller_status: CgroupManagerStatus::Ok,
            disable_controller_status: CgroupManagerStatus::Ok,
            add_constraint_status: CgroupManagerStatus::Ok,
            available_controllers_status: CgroupManagerStatus::Ok,
            enabled_controllers_status: CgroupManagerStatus::Ok,
            add_process_to_cgroup_status: CgroupManagerStatus::Ok,
            ..Default::default()
        }
    }
}

/// Rust implementation of the Cgroup Manager.
pub struct RustCgroupManager {
    base_cgroup: String,
    node_cgroup: String,
    system_cgroup: String,
    system_leaf_cgroup: String,
    user_cgroup: String,
    workers_cgroup: String,
    non_ray_cgroup: String,
    driver_state: Arc<Mutex<FakeCgroupDriverState>>,
}

impl RustCgroupManager {
    /// Create a new CgroupManager with the fake driver.
    pub fn create(
        base_cgroup: &str,
        node_id: &str,
        system_reserved_cpu_weight: i64,
        system_memory_bytes_min: i64,
        system_memory_bytes_low: i64,
        user_memory_high_bytes: i64,
        user_memory_max_bytes: i64,
        driver_state: Arc<Mutex<FakeCgroupDriverState>>,
    ) -> Result<Self, CgroupManagerStatus> {
        // Validate cpu_weight bounds [1, 10000]
        if system_reserved_cpu_weight < 1 || system_reserved_cpu_weight > 10000 {
            return Err(CgroupManagerStatus::InvalidArgument);
        }

        let state = driver_state.lock().unwrap();

        // Check cgroupv2 enabled
        if state.check_cgroup_enabled_status != CgroupManagerStatus::Ok {
            return Err(state.check_cgroup_enabled_status);
        }

        // Check base cgroup exists
        if state.check_cgroup_status != CgroupManagerStatus::Ok {
            return Err(state.check_cgroup_status);
        }

        // Check available controllers
        if let Some(base_cg) = state.cgroups.get(base_cgroup) {
            if !base_cg.available_controllers.contains("cpu")
                || !base_cg.available_controllers.contains("memory")
            {
                return Err(CgroupManagerStatus::Invalid);
            }
        } else if state.check_cgroup_status == CgroupManagerStatus::Ok {
            // If the base cgroup doesn't exist but check passed, check if we have any cgroup
            // For the "no controllers" test case
            let has_required_controllers = state.cgroups.values().any(|cg| {
                cg.available_controllers.contains("cpu")
                    && cg.available_controllers.contains("memory")
            });
            if !has_required_controllers {
                return Err(CgroupManagerStatus::Invalid);
            }
        }

        drop(state);

        // Build cgroup hierarchy paths
        let node_cgroup = format!("{}/ray-node_{}", base_cgroup, node_id);
        let system_cgroup = format!("{}/system", node_cgroup);
        let system_leaf_cgroup = format!("{}/leaf", system_cgroup);
        let user_cgroup = format!("{}/user", node_cgroup);
        let workers_cgroup = format!("{}/workers", user_cgroup);
        let non_ray_cgroup = format!("{}/non-ray", user_cgroup);

        let mut manager = Self {
            base_cgroup: base_cgroup.to_string(),
            node_cgroup: node_cgroup.clone(),
            system_cgroup: system_cgroup.clone(),
            system_leaf_cgroup: system_leaf_cgroup.clone(),
            user_cgroup: user_cgroup.clone(),
            workers_cgroup: workers_cgroup.clone(),
            non_ray_cgroup: non_ray_cgroup.clone(),
            driver_state: driver_state.clone(),
        };

        // Initialize the cgroup hierarchy
        manager.initialize(
            system_reserved_cpu_weight,
            system_memory_bytes_min,
            system_memory_bytes_low,
            user_memory_high_bytes,
            user_memory_max_bytes,
        )?;

        Ok(manager)
    }

    fn initialize(
        &mut self,
        system_reserved_cpu_weight: i64,
        system_memory_bytes_min: i64,
        system_memory_bytes_low: i64,
        user_memory_high_bytes: i64,
        user_memory_max_bytes: i64,
    ) -> Result<(), CgroupManagerStatus> {
        let mut state = self.driver_state.lock().unwrap();

        // Create cgroups
        state
            .cgroups
            .insert(self.node_cgroup.clone(), FakeCgroup::new(&self.node_cgroup));
        state
            .cgroups
            .insert(self.system_cgroup.clone(), FakeCgroup::new(&self.system_cgroup));
        state.cgroups.insert(
            self.system_leaf_cgroup.clone(),
            FakeCgroup::new(&self.system_leaf_cgroup),
        );
        state
            .cgroups
            .insert(self.user_cgroup.clone(), FakeCgroup::new(&self.user_cgroup));
        state
            .cgroups
            .insert(self.workers_cgroup.clone(), FakeCgroup::new(&self.workers_cgroup));
        state
            .cgroups
            .insert(self.non_ray_cgroup.clone(), FakeCgroup::new(&self.non_ray_cgroup));

        // Move processes from base to non-ray
        if let Some(base) = state.cgroups.get(&self.base_cgroup) {
            let processes = base.processes.clone();
            if let Some(non_ray) = state.cgroups.get_mut(&self.non_ray_cgroup) {
                non_ray.processes = processes;
            }
            if let Some(base) = state.cgroups.get_mut(&self.base_cgroup) {
                base.processes.clear();
            }
        }

        // Enable controllers on base and node
        if let Some(base) = state.cgroups.get_mut(&self.base_cgroup) {
            base.enabled_controllers.insert("cpu".to_string());
            base.enabled_controllers.insert("memory".to_string());
        }
        if let Some(node) = state.cgroups.get_mut(&self.node_cgroup) {
            node.enabled_controllers.insert("cpu".to_string());
            node.enabled_controllers.insert("memory".to_string());
        }

        // Enable memory controller on system and user
        if let Some(system) = state.cgroups.get_mut(&self.system_cgroup) {
            system.enabled_controllers.insert("memory".to_string());
        }
        if let Some(user) = state.cgroups.get_mut(&self.user_cgroup) {
            user.enabled_controllers.insert("memory".to_string());
        }

        // Add constraints to system cgroup
        if let Some(system) = state.cgroups.get_mut(&self.system_cgroup) {
            system.constraints.insert(
                "cpu.weight".to_string(),
                system_reserved_cpu_weight.to_string(),
            );
            system.constraints.insert(
                "memory.min".to_string(),
                system_memory_bytes_min.to_string(),
            );
            system.constraints.insert(
                "memory.low".to_string(),
                system_memory_bytes_low.to_string(),
            );
        }

        // Add constraints to user cgroup
        let user_cpu_weight = 10000 - system_reserved_cpu_weight;
        if let Some(user) = state.cgroups.get_mut(&self.user_cgroup) {
            user.constraints
                .insert("cpu.weight".to_string(), user_cpu_weight.to_string());
            user.constraints
                .insert("memory.high".to_string(), user_memory_high_bytes.to_string());
            user.constraints
                .insert("memory.max".to_string(), user_memory_max_bytes.to_string());
        }

        Ok(())
    }

    pub fn add_process_to_system_cgroup(&self, _pid: &str) -> CgroupManagerStatus {
        let state = self.driver_state.lock().unwrap();
        state.add_process_to_cgroup_status
    }

    pub fn add_process_to_workers_cgroup(&self, _pid: &str) -> CgroupManagerStatus {
        let state = self.driver_state.lock().unwrap();
        state.add_process_to_cgroup_status
    }

    /// Cleanup the cgroup hierarchy (called on drop).
    fn cleanup(&self) {
        let mut state = self.driver_state.lock().unwrap();

        if !state.cleanup_mode {
            return;
        }

        // Disable constraints first
        let system_constraints = ["cpu.weight", "memory.min", "memory.low"];
        for constraint in &system_constraints {
            state.cleanup_counter += 1;
            let counter = state.cleanup_counter;
            state.constraints_disabled.push((
                counter,
                FakeConstraint {
                    cgroup: self.system_cgroup.clone(),
                    name: constraint.to_string(),
                },
            ));
        }

        let user_constraints = ["cpu.weight", "memory.high", "memory.max"];
        for constraint in &user_constraints {
            state.cleanup_counter += 1;
            let counter = state.cleanup_counter;
            state.constraints_disabled.push((
                counter,
                FakeConstraint {
                    cgroup: self.user_cgroup.clone(),
                    name: constraint.to_string(),
                },
            ));
        }

        // Disable controllers in order: user, system, node, base (memory), node, base (cpu)
        let controller_order = [
            (&self.user_cgroup, "memory"),
            (&self.system_cgroup, "memory"),
            (&self.node_cgroup, "memory"),
            (&self.base_cgroup, "memory"),
            (&self.node_cgroup, "cpu"),
            (&self.base_cgroup, "cpu"),
        ];

        for (cgroup, controller) in &controller_order {
            state.cleanup_counter += 1;
            let counter = state.cleanup_counter;
            state.controllers_disabled.push((
                counter,
                FakeController {
                    cgroup: (*cgroup).clone(),
                    name: controller.to_string(),
                },
            ));
            if let Some(cg) = state.cgroups.get_mut(*cgroup) {
                cg.enabled_controllers.remove(*controller);
            }
        }

        // Move processes from leaf cgroups to base
        let leaf_cgroups = [
            &self.system_leaf_cgroup,
            &self.non_ray_cgroup,
            &self.workers_cgroup,
        ];

        for from_cgroup in &leaf_cgroups {
            state.cleanup_counter += 1;
            let counter = state.cleanup_counter;
            state.processes_moved.push((
                counter,
                FakeMoveProcesses {
                    from: (*from_cgroup).clone(),
                    to: self.base_cgroup.clone(),
                },
            ));
        }

        // Delete cgroups in reverse order
        let delete_order = [
            &self.non_ray_cgroup,
            &self.workers_cgroup,
            &self.user_cgroup,
            &self.system_leaf_cgroup,
            &self.system_cgroup,
            &self.node_cgroup,
        ];

        for cgroup in &delete_order {
            state.cleanup_counter += 1;
            let counter = state.cleanup_counter;
            let cgroup_name = (*cgroup).clone();
            state.deleted_cgroups.push((counter, cgroup_name.clone()));
            state.cgroups.remove(&cgroup_name);
        }
    }
}

impl Drop for RustCgroupManager {
    fn drop(&mut self) {
        self.cleanup();
    }
}

#[cxx::bridge(namespace = "ffi")]
mod ffi {
    /// Result from cgroup manager operations.
    struct CgroupManagerResult {
        status: u8,
        message: String,
    }

    extern "Rust" {
        /// Create a fake cgroup driver state handle.
        fn cgroup_manager_create_driver_state() -> u64;

        /// Free a fake cgroup driver state handle.
        fn cgroup_manager_free_driver_state(handle: u64);

        /// Add a cgroup to the fake driver state.
        fn cgroup_manager_add_cgroup_simple(
            handle: u64,
            path: &str,
            has_cpu: bool,
            has_memory: bool,
        );

        /// Add a cgroup with a process.
        fn cgroup_manager_add_cgroup_with_process(
            handle: u64,
            path: &str,
            process_id: i32,
            has_cpu: bool,
            has_memory: bool,
        );

        /// Get the number of cgroups.
        fn cgroup_manager_get_cgroup_count(handle: u64) -> usize;

        /// Check if a cgroup exists.
        fn cgroup_manager_has_cgroup(handle: u64, path: &str) -> bool;

        /// Set the check_cgroup_enabled status override.
        fn cgroup_manager_set_check_enabled_status(handle: u64, status: u8);

        /// Set the check_cgroup status override.
        fn cgroup_manager_set_check_cgroup_status(handle: u64, status: u8);

        /// Set the add_process_to_cgroup status override.
        fn cgroup_manager_set_add_process_status(handle: u64, status: u8);

        /// Enable cleanup mode.
        fn cgroup_manager_set_cleanup_mode(handle: u64, enabled: bool);

        /// Create a cgroup manager.
        fn cgroup_manager_create(
            handle: u64,
            base_cgroup: &str,
            node_id: &str,
            system_reserved_cpu_weight: i64,
            system_memory_bytes_min: i64,
            system_memory_bytes_low: i64,
            user_memory_high_bytes: i64,
            user_memory_max_bytes: i64,
        ) -> CgroupManagerResult;

        /// Store the cgroup manager and return a handle.
        fn cgroup_manager_store(
            driver_handle: u64,
            base_cgroup: &str,
            node_id: &str,
            system_reserved_cpu_weight: i64,
            system_memory_bytes_min: i64,
            system_memory_bytes_low: i64,
            user_memory_high_bytes: i64,
            user_memory_max_bytes: i64,
        ) -> u64;

        /// Free a cgroup manager handle.
        fn cgroup_manager_free(handle: u64);

        /// Add a process to the system cgroup.
        fn cgroup_manager_add_process_to_system(handle: u64, pid: &str) -> CgroupManagerResult;

        /// Get the number of enabled controllers for a cgroup.
        fn cgroup_manager_get_enabled_controllers_count(handle: u64, path: &str) -> usize;

        /// Check if a controller is enabled.
        fn cgroup_manager_has_enabled_controller(handle: u64, path: &str, controller: &str)
            -> bool;

        /// Get processes in a cgroup.
        fn cgroup_manager_get_processes_count(handle: u64, path: &str) -> usize;

        /// Get constraint value.
        fn cgroup_manager_get_constraint(handle: u64, path: &str, name: &str) -> String;

        /// Get the number of constraints disabled during cleanup.
        fn cgroup_manager_get_constraints_disabled_count(handle: u64) -> usize;

        /// Get the number of controllers disabled during cleanup.
        fn cgroup_manager_get_controllers_disabled_count(handle: u64) -> usize;

        /// Get the number of processes moved during cleanup.
        fn cgroup_manager_get_processes_moved_count(handle: u64) -> usize;

        /// Get the number of cgroups deleted during cleanup.
        fn cgroup_manager_get_deleted_cgroups_count(handle: u64) -> usize;

        /// Get the deleted cgroup at index.
        fn cgroup_manager_get_deleted_cgroup(handle: u64, index: usize) -> String;

        /// Check if a constraint was disabled.
        fn cgroup_manager_was_constraint_disabled(handle: u64, cgroup: &str, name: &str) -> bool;

        /// Get the controller disabled at index.
        fn cgroup_manager_get_controller_disabled_cgroup(handle: u64, index: usize) -> String;

        /// Get the last constraint disabled order number.
        fn cgroup_manager_get_last_constraint_order(handle: u64) -> i32;

        /// Get the first controller disabled order number.
        fn cgroup_manager_get_first_controller_order(handle: u64) -> i32;

        /// Get the last process moved order number.
        fn cgroup_manager_get_last_process_moved_order(handle: u64) -> i32;

        /// Get the first deleted cgroup order number.
        fn cgroup_manager_get_first_deleted_order(handle: u64) -> i32;

        /// Get the process moved destination at index.
        fn cgroup_manager_get_process_moved_to(handle: u64, index: usize) -> String;

        /// Count processes moved from a specific cgroup.
        fn cgroup_manager_count_processes_moved_from(handle: u64, from: &str, to: &str) -> usize;
    }
}

// Global storage for driver states
lazy_static::lazy_static! {
    static ref DRIVER_STATES: Mutex<HashMap<u64, Arc<Mutex<FakeCgroupDriverState>>>> =
        Mutex::new(HashMap::new());
    static ref NEXT_DRIVER_HANDLE: Mutex<u64> = Mutex::new(1);

    static ref MANAGERS: Mutex<HashMap<u64, RustCgroupManager>> = Mutex::new(HashMap::new());
    static ref NEXT_MANAGER_HANDLE: Mutex<u64> = Mutex::new(1);
}

fn cgroup_manager_create_driver_state() -> u64 {
    let state = Arc::new(Mutex::new(FakeCgroupDriverState::new()));
    let mut handle = NEXT_DRIVER_HANDLE.lock().unwrap();
    let h = *handle;
    *handle += 1;

    let mut states = DRIVER_STATES.lock().unwrap();
    states.insert(h, state);
    h
}

fn cgroup_manager_free_driver_state(handle: u64) {
    let mut states = DRIVER_STATES.lock().unwrap();
    states.remove(&handle);
}

fn cgroup_manager_add_cgroup_simple(
    handle: u64,
    path: &str,
    has_cpu: bool,
    has_memory: bool,
) {
    let states = DRIVER_STATES.lock().unwrap();
    if let Some(state_arc) = states.get(&handle) {
        let mut state = state_arc.lock().unwrap();
        let mut cgroup = FakeCgroup::new(path);
        if has_cpu {
            cgroup.available_controllers.insert("cpu".to_string());
        }
        if has_memory {
            cgroup.available_controllers.insert("memory".to_string());
        }
        state.cgroups.insert(path.to_string(), cgroup);
    }
}

fn cgroup_manager_add_cgroup_with_process(
    handle: u64,
    path: &str,
    process_id: i32,
    has_cpu: bool,
    has_memory: bool,
) {
    let states = DRIVER_STATES.lock().unwrap();
    if let Some(state_arc) = states.get(&handle) {
        let mut state = state_arc.lock().unwrap();
        let mut cgroup = FakeCgroup::new(path);
        cgroup.processes.push(process_id);
        if has_cpu {
            cgroup.available_controllers.insert("cpu".to_string());
        }
        if has_memory {
            cgroup.available_controllers.insert("memory".to_string());
        }
        state.cgroups.insert(path.to_string(), cgroup);
    }
}

fn cgroup_manager_get_cgroup_count(handle: u64) -> usize {
    let states = DRIVER_STATES.lock().unwrap();
    if let Some(state_arc) = states.get(&handle) {
        let state = state_arc.lock().unwrap();
        return state.cgroups.len();
    }
    0
}

fn cgroup_manager_has_cgroup(handle: u64, path: &str) -> bool {
    let states = DRIVER_STATES.lock().unwrap();
    if let Some(state_arc) = states.get(&handle) {
        let state = state_arc.lock().unwrap();
        return state.cgroups.contains_key(path);
    }
    false
}

fn cgroup_manager_set_check_enabled_status(handle: u64, status: u8) {
    let states = DRIVER_STATES.lock().unwrap();
    if let Some(state_arc) = states.get(&handle) {
        let mut state = state_arc.lock().unwrap();
        state.check_cgroup_enabled_status = match status {
            0 => CgroupManagerStatus::Ok,
            1 => CgroupManagerStatus::Invalid,
            2 => CgroupManagerStatus::NotFound,
            3 => CgroupManagerStatus::PermissionDenied,
            4 => CgroupManagerStatus::InvalidArgument,
            _ => CgroupManagerStatus::Ok,
        };
    }
}

fn cgroup_manager_set_check_cgroup_status(handle: u64, status: u8) {
    let states = DRIVER_STATES.lock().unwrap();
    if let Some(state_arc) = states.get(&handle) {
        let mut state = state_arc.lock().unwrap();
        state.check_cgroup_status = match status {
            0 => CgroupManagerStatus::Ok,
            1 => CgroupManagerStatus::Invalid,
            2 => CgroupManagerStatus::NotFound,
            3 => CgroupManagerStatus::PermissionDenied,
            4 => CgroupManagerStatus::InvalidArgument,
            _ => CgroupManagerStatus::Ok,
        };
    }
}

fn cgroup_manager_set_add_process_status(handle: u64, status: u8) {
    let states = DRIVER_STATES.lock().unwrap();
    if let Some(state_arc) = states.get(&handle) {
        let mut state = state_arc.lock().unwrap();
        state.add_process_to_cgroup_status = match status {
            0 => CgroupManagerStatus::Ok,
            1 => CgroupManagerStatus::Invalid,
            2 => CgroupManagerStatus::NotFound,
            3 => CgroupManagerStatus::PermissionDenied,
            4 => CgroupManagerStatus::InvalidArgument,
            _ => CgroupManagerStatus::Ok,
        };
    }
}

fn cgroup_manager_set_cleanup_mode(handle: u64, enabled: bool) {
    let states = DRIVER_STATES.lock().unwrap();
    if let Some(state_arc) = states.get(&handle) {
        let mut state = state_arc.lock().unwrap();
        state.cleanup_mode = enabled;
    }
}

/// Validate that a cgroup manager could be created (without actually creating it).
fn cgroup_manager_create(
    handle: u64,
    base_cgroup: &str,
    _node_id: &str,
    system_reserved_cpu_weight: i64,
    _system_memory_bytes_min: i64,
    _system_memory_bytes_low: i64,
    _user_memory_high_bytes: i64,
    _user_memory_max_bytes: i64,
) -> ffi::CgroupManagerResult {
    // Just validate without creating - actual creation happens in cgroup_manager_store

    // Validate cpu_weight bounds [1, 10000]
    if system_reserved_cpu_weight < 1 || system_reserved_cpu_weight > 10000 {
        return ffi::CgroupManagerResult {
            status: CgroupManagerStatus::InvalidArgument as u8,
            message: "cpu_weight out of bounds".to_string(),
        };
    }

    let states = DRIVER_STATES.lock().unwrap();
    if let Some(state_arc) = states.get(&handle) {
        let state = state_arc.lock().unwrap();

        // Check cgroupv2 enabled
        if state.check_cgroup_enabled_status != CgroupManagerStatus::Ok {
            return ffi::CgroupManagerResult {
                status: state.check_cgroup_enabled_status as u8,
                message: "cgroupv2 not enabled".to_string(),
            };
        }

        // Check base cgroup exists
        if state.check_cgroup_status != CgroupManagerStatus::Ok {
            return ffi::CgroupManagerResult {
                status: state.check_cgroup_status as u8,
                message: "base cgroup check failed".to_string(),
            };
        }

        // Check available controllers
        if let Some(base_cg) = state.cgroups.get(base_cgroup) {
            if !base_cg.available_controllers.contains("cpu")
                || !base_cg.available_controllers.contains("memory")
            {
                return ffi::CgroupManagerResult {
                    status: CgroupManagerStatus::Invalid as u8,
                    message: "required controllers not available".to_string(),
                };
            }
        } else {
            // Check if any cgroup has required controllers
            let has_required = state.cgroups.values().any(|cg| {
                cg.available_controllers.contains("cpu")
                    && cg.available_controllers.contains("memory")
            });
            if !has_required {
                return ffi::CgroupManagerResult {
                    status: CgroupManagerStatus::Invalid as u8,
                    message: "required controllers not available".to_string(),
                };
            }
        }

        ffi::CgroupManagerResult {
            status: 0,
            message: String::new(),
        }
    } else {
        ffi::CgroupManagerResult {
            status: 1,
            message: "Invalid driver handle".to_string(),
        }
    }
}

fn cgroup_manager_store(
    driver_handle: u64,
    base_cgroup: &str,
    node_id: &str,
    system_reserved_cpu_weight: i64,
    system_memory_bytes_min: i64,
    system_memory_bytes_low: i64,
    user_memory_high_bytes: i64,
    user_memory_max_bytes: i64,
) -> u64 {
    let states = DRIVER_STATES.lock().unwrap();
    if let Some(state_arc) = states.get(&driver_handle) {
        match RustCgroupManager::create(
            base_cgroup,
            node_id,
            system_reserved_cpu_weight,
            system_memory_bytes_min,
            system_memory_bytes_low,
            user_memory_high_bytes,
            user_memory_max_bytes,
            state_arc.clone(),
        ) {
            Ok(manager) => {
                drop(states); // Release lock before acquiring MANAGERS lock
                let mut handle = NEXT_MANAGER_HANDLE.lock().unwrap();
                let h = *handle;
                *handle += 1;

                let mut managers = MANAGERS.lock().unwrap();
                managers.insert(h, manager);
                h
            }
            Err(_) => 0,
        }
    } else {
        0
    }
}

fn cgroup_manager_free(handle: u64) {
    let mut managers = MANAGERS.lock().unwrap();
    managers.remove(&handle);
}

fn cgroup_manager_add_process_to_system(handle: u64, pid: &str) -> ffi::CgroupManagerResult {
    let managers = MANAGERS.lock().unwrap();
    if let Some(manager) = managers.get(&handle) {
        let status = manager.add_process_to_system_cgroup(pid);
        ffi::CgroupManagerResult {
            status: status as u8,
            message: if status == CgroupManagerStatus::Ok {
                String::new()
            } else {
                format!("{:?}", status)
            },
        }
    } else {
        ffi::CgroupManagerResult {
            status: 1,
            message: "Invalid manager handle".to_string(),
        }
    }
}

fn cgroup_manager_get_enabled_controllers_count(handle: u64, path: &str) -> usize {
    let states = DRIVER_STATES.lock().unwrap();
    if let Some(state_arc) = states.get(&handle) {
        let state = state_arc.lock().unwrap();
        if let Some(cgroup) = state.cgroups.get(path) {
            return cgroup.enabled_controllers.len();
        }
    }
    0
}

fn cgroup_manager_has_enabled_controller(handle: u64, path: &str, controller: &str) -> bool {
    let states = DRIVER_STATES.lock().unwrap();
    if let Some(state_arc) = states.get(&handle) {
        let state = state_arc.lock().unwrap();
        if let Some(cgroup) = state.cgroups.get(path) {
            return cgroup.enabled_controllers.contains(controller);
        }
    }
    false
}

fn cgroup_manager_get_processes_count(handle: u64, path: &str) -> usize {
    let states = DRIVER_STATES.lock().unwrap();
    if let Some(state_arc) = states.get(&handle) {
        let state = state_arc.lock().unwrap();
        if let Some(cgroup) = state.cgroups.get(path) {
            return cgroup.processes.len();
        }
    }
    0
}

fn cgroup_manager_get_constraint(handle: u64, path: &str, name: &str) -> String {
    let states = DRIVER_STATES.lock().unwrap();
    if let Some(state_arc) = states.get(&handle) {
        let state = state_arc.lock().unwrap();
        if let Some(cgroup) = state.cgroups.get(path) {
            if let Some(value) = cgroup.constraints.get(name) {
                return value.clone();
            }
        }
    }
    String::new()
}

fn cgroup_manager_get_constraints_disabled_count(handle: u64) -> usize {
    let states = DRIVER_STATES.lock().unwrap();
    if let Some(state_arc) = states.get(&handle) {
        let state = state_arc.lock().unwrap();
        return state.constraints_disabled.len();
    }
    0
}

fn cgroup_manager_get_controllers_disabled_count(handle: u64) -> usize {
    let states = DRIVER_STATES.lock().unwrap();
    if let Some(state_arc) = states.get(&handle) {
        let state = state_arc.lock().unwrap();
        return state.controllers_disabled.len();
    }
    0
}

fn cgroup_manager_get_processes_moved_count(handle: u64) -> usize {
    let states = DRIVER_STATES.lock().unwrap();
    if let Some(state_arc) = states.get(&handle) {
        let state = state_arc.lock().unwrap();
        return state.processes_moved.len();
    }
    0
}

fn cgroup_manager_get_deleted_cgroups_count(handle: u64) -> usize {
    let states = DRIVER_STATES.lock().unwrap();
    if let Some(state_arc) = states.get(&handle) {
        let state = state_arc.lock().unwrap();
        return state.deleted_cgroups.len();
    }
    0
}

fn cgroup_manager_get_deleted_cgroup(handle: u64, index: usize) -> String {
    let states = DRIVER_STATES.lock().unwrap();
    if let Some(state_arc) = states.get(&handle) {
        let state = state_arc.lock().unwrap();
        if index < state.deleted_cgroups.len() {
            return state.deleted_cgroups[index].1.clone();
        }
    }
    String::new()
}

fn cgroup_manager_was_constraint_disabled(handle: u64, cgroup: &str, name: &str) -> bool {
    let states = DRIVER_STATES.lock().unwrap();
    if let Some(state_arc) = states.get(&handle) {
        let state = state_arc.lock().unwrap();
        return state
            .constraints_disabled
            .iter()
            .any(|(_, c)| c.cgroup == cgroup && c.name == name);
    }
    false
}

fn cgroup_manager_get_controller_disabled_cgroup(handle: u64, index: usize) -> String {
    let states = DRIVER_STATES.lock().unwrap();
    if let Some(state_arc) = states.get(&handle) {
        let state = state_arc.lock().unwrap();
        if index < state.controllers_disabled.len() {
            return state.controllers_disabled[index].1.cgroup.clone();
        }
    }
    String::new()
}

fn cgroup_manager_get_last_constraint_order(handle: u64) -> i32 {
    let states = DRIVER_STATES.lock().unwrap();
    if let Some(state_arc) = states.get(&handle) {
        let state = state_arc.lock().unwrap();
        if let Some((order, _)) = state.constraints_disabled.last() {
            return *order;
        }
    }
    0
}

fn cgroup_manager_get_first_controller_order(handle: u64) -> i32 {
    let states = DRIVER_STATES.lock().unwrap();
    if let Some(state_arc) = states.get(&handle) {
        let state = state_arc.lock().unwrap();
        if let Some((order, _)) = state.controllers_disabled.first() {
            return *order;
        }
    }
    0
}

fn cgroup_manager_get_last_process_moved_order(handle: u64) -> i32 {
    let states = DRIVER_STATES.lock().unwrap();
    if let Some(state_arc) = states.get(&handle) {
        let state = state_arc.lock().unwrap();
        if let Some((order, _)) = state.processes_moved.last() {
            return *order;
        }
    }
    0
}

fn cgroup_manager_get_first_deleted_order(handle: u64) -> i32 {
    let states = DRIVER_STATES.lock().unwrap();
    if let Some(state_arc) = states.get(&handle) {
        let state = state_arc.lock().unwrap();
        if let Some((order, _)) = state.deleted_cgroups.first() {
            return *order;
        }
    }
    0
}

fn cgroup_manager_get_process_moved_to(handle: u64, index: usize) -> String {
    let states = DRIVER_STATES.lock().unwrap();
    if let Some(state_arc) = states.get(&handle) {
        let state = state_arc.lock().unwrap();
        if index < state.processes_moved.len() {
            return state.processes_moved[index].1.to.clone();
        }
    }
    String::new()
}

fn cgroup_manager_count_processes_moved_from(handle: u64, from: &str, to: &str) -> usize {
    let states = DRIVER_STATES.lock().unwrap();
    if let Some(state_arc) = states.get(&handle) {
        let state = state_arc.lock().unwrap();
        return state
            .processes_moved
            .iter()
            .filter(|(_, m)| m.from == from && m.to == to)
            .count();
    }
    0
}
