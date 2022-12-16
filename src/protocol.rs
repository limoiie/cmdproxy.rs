use crate::params::Param;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use typed_builder::TypedBuilder;

#[derive(Debug, Clone, Serialize, Deserialize, TypedBuilder)]
pub struct RunRequest {
    pub command: Param,
    pub args: Vec<Param>,
    #[builder(default, setter(strip_option))]
    pub cwd: Option<String>,
    #[builder(default, setter(strip_option))]
    pub env: Option<HashMap<String, Param>>,
    #[builder(default, setter(strip_option))]
    pub to_downloads: Option<Vec<Param>>,
    #[builder(default, setter(strip_option))]
    pub to_uploads: Option<Vec<Param>>,
    #[builder(default, setter(strip_option))]
    pub stdout: Option<Param>,
    #[builder(default, setter(strip_option))]
    pub stderr: Option<Param>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RunResponse {
    pub return_code: i32,
    pub exc: Option<String>,
}
