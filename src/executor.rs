// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use crate::errors::BallistaError;
use crate::utils::wait_for_future;
use ballista_core::config::{LogRotationPolicy, TaskSchedulingPolicy};
use ballista_executor::executor_process::{start_executor_process, ExecutorProcessConfig};
use pyo3::prelude::*;

/// Python wrapper around an executor, allowing users to run an executor within a Python process.
#[pyclass(name = "Executor", module = "ballista", subclass)]
pub struct PyExecutor {}

#[pymethods]
impl PyExecutor {
    #[new]
    #[pyo3(signature = (
        scheduler_host,
        scheduler_port,
        bind_host,
        bind_port,
        grpc_port,
        concurrent_tasks,
    ))]
    fn new(
        scheduler_host: &str,
        scheduler_port: u16,
        bind_host: &str,
        bind_port: u16,
        grpc_port: u16,
        concurrent_tasks: usize,
        py: Python,
    ) -> PyResult<Self> {
        env_logger::init();

        // TODO add option to register a custom query stage executor ExecutionPlan so
        // that we can execute Python plans (delegating to DataFrame libraries)

        let config = ExecutorProcessConfig {
            special_mod_log_level: "info".to_string(),
            external_host: None,
            bind_host: bind_host.to_string(),
            port: bind_port,
            grpc_port,
            scheduler_host: scheduler_host.to_string(),
            scheduler_port,
            scheduler_connect_timeout_seconds: 60,
            concurrent_tasks,
            task_scheduling_policy: TaskSchedulingPolicy::PullStaged,
            work_dir: None,
            log_dir: None,
            log_file_name_prefix: "".to_string(),
            log_rotation_policy: LogRotationPolicy::Daily,
            print_thread_info: true,
            job_data_ttl_seconds: 60 * 60,
            job_data_clean_up_interval_seconds: 60 * 30,
        };

        let fut = start_executor_process(config);
        let _ = wait_for_future(py, fut).map_err(|e| BallistaError::Common(format!("{}", e)))?;

        Ok(Self {})
    }
}
