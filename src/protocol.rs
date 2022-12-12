use crate::params::Param;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RunRequest {
    pub command: Param,
    pub args: Vec<Param>,
    pub cwd: Option<String>,
    pub env: Option<HashMap<String, Param>>,
    // todo: support to specify relative path in Param
    pub to_downloads: Option<Vec<Param>>,
    pub to_uploads: Option<Vec<Param>>,
    pub stdout: Option<Param>,
    pub stderr: Option<Param>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RunResponse {
    pub return_code: i32,
    pub exc: Option<String>,
}
