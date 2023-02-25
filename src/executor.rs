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
use ballista_core::execution_plans::ShuffleWriterExec;
use ballista_executor::executor::ExecutionEngine;
use ballista_executor::executor_process::{start_executor_process, ExecutorProcessConfig};
use datafusion::physical_plan::ExecutionPlan;
use datafusion_substrait::physical_plan::producer::to_substrait_rel;
use datafusion_substrait::substrait::proto::{extensions, plan_rel, Plan, PlanRel, RelRoot};
use prost::Message;
use pyo3::prelude::*;
use pyo3::types::{IntoPyDict, PyTuple};
use std::collections::HashMap;
use std::sync::Arc;

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
            execution_engine: Some(Arc::new(PythonExecutionEngine {})),
        };

        let fut = start_executor_process(config);
        let _ = wait_for_future(py, fut).map_err(|e| BallistaError::Common(format!("{}", e)))?;

        Ok(Self {})
    }
}

struct PythonExecutionEngine {}

impl ExecutionEngine for PythonExecutionEngine {
    fn new_shuffle_writer(
        &self,
        job_id: String,
        stage_id: usize,
        plan: Arc<dyn ExecutionPlan>,
        work_dir: &str,
    ) -> Result<Arc<ShuffleWriterExec>, ballista_core::error::BallistaError> {
        // serialize the plan to substrait format
        let substrait_plan = to_substrait_plan(plan.as_ref())
            .map_err(|e| ballista_core::error::BallistaError::General(format!("{}", e)))?;
        let mut plan_bytes = Vec::<u8>::new();
        substrait_plan.encode(&mut plan_bytes).map_err(|e| {
            ballista_core::error::BallistaError::General(format!(
                "Failed to encode substrait plan: {e}"
            ))
        })?;

        // call Python code to execute the substrait plan
        Python::with_gil(|py| {
            let fun: Py<PyAny> = PyModule::from_code(
                py,
                "def execute_substrait(*args, **kwargs):
                    print('[Python] execute_substrait with', len(kwargs['plan_bytes']), 'bytes'",
                "",
                "",
            )?
            .getattr("execute_substrait")?
            .into();

            let py_bytes = plan_bytes.into_py(py);
            let kwargs = vec![("plan_bytes", py_bytes)];
            fun.call(py, (), Some(kwargs.into_py_dict(py)))?;

            // TODO how do we return a stream of batches here? or maybe we need to do the
            // shuffle write in Python as well?

            Ok(())
        })
        .map_err(|e: PyErr| {
            ballista_core::error::BallistaError::General("python fail".to_string())
        })?;

        Err(ballista_core::error::BallistaError::NotImplemented(
            "PythonExecutionEngine not implemented yet".to_string(),
        ))
    }
}

// TODO upstream this into datafusion-substrait
/// Convert DataFusion ExecutionPlan to Substrait Plan
pub fn to_substrait_plan(plan: &dyn ExecutionPlan) -> Result<Box<Plan>, BallistaError> {
    // Parse relation nodes
    let mut extension_info: (
        Vec<extensions::SimpleExtensionDeclaration>,
        HashMap<String, u32>,
    ) = (vec![], HashMap::new());
    // Generate PlanRel(s)
    // Note: Only 1 relation tree is currently supported
    let plan_rels = vec![PlanRel {
        rel_type: Some(plan_rel::RelType::Root(RelRoot {
            input: Some(*to_substrait_rel(plan, &mut extension_info)?),
            names: plan
                .schema()
                .fields()
                .iter()
                .map(|f| f.name().clone())
                .collect(),
        })),
    }];

    let (function_extensions, _) = extension_info;

    // Return parsed plan
    Ok(Box::new(Plan {
        version: None, // TODO: https://github.com/apache/arrow-datafusion/issues/4949
        extension_uris: vec![],
        extensions: function_extensions,
        relations: plan_rels,
        advanced_extensions: None,
        expected_type_urls: vec![],
    }))
}
