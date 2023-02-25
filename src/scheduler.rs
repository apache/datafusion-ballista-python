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
use ballista_core::config::TaskSchedulingPolicy;
use ballista_scheduler::cluster::BallistaCluster;
use ballista_scheduler::config::{ClusterStorageConfig, SchedulerConfig, SlotsPolicy};
use ballista_scheduler::scheduler_process::start_server;
use log::info;
use pyo3::prelude::*;

/// Python wrapper around a scheduler, allowing users to run an scheduler within a Python process.
#[pyclass(name = "Scheduler", module = "ballista", subclass)]
pub struct PyScheduler {}

#[pymethods]
impl PyScheduler {
    #[new]
    #[pyo3(signature = (
    bind_host,
    bind_port,
    external_host,
    ))]
    fn new(bind_host: &str, bind_port: u16, external_host: &str, py: Python) -> PyResult<Self> {
        env_logger::init();

        info!("Starting scheduler on {bind_host}:{bind_port}");

        let addr = format!("{}:{}", bind_host, bind_port);
        let addr = addr.parse()?;

        let config = SchedulerConfig {
            namespace: "default".to_string(),
            external_host: external_host.to_string(),
            bind_port: bind_port,
            scheduling_policy: TaskSchedulingPolicy::PullStaged,
            event_loop_buffer_size: 1000,
            executor_slots_policy: SlotsPolicy::RoundRobin,
            finished_job_data_clean_up_interval_seconds: 60,
            finished_job_state_clean_up_interval_seconds: 60,
            advertise_flight_sql_endpoint: None,
            cluster_storage: ClusterStorageConfig::Memory,
            job_resubmit_interval_ms: None,
        };

        let cluster = BallistaCluster::new_from_config(&config);
        let cluster =
            wait_for_future(py, cluster).map_err(|e| BallistaError::Common(format!("{}", e)))?;

        let fut = start_server(cluster, addr, config);
        let _ = wait_for_future(py, fut).map_err(|e| BallistaError::Common(format!("{}", e)))?;

        Ok(Self {})
    }
}
