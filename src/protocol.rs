use std::collections::HashMap;

use serde::{Deserialize, Serialize};
use typed_builder::TypedBuilder;

use crate::params::Param;

#[derive(Debug, Clone, Serialize, Deserialize, TypedBuilder)]
pub struct RunSpecification<P> {
    pub command: P,
    pub args: Vec<P>,
    #[builder(default, setter(strip_option))]
    pub cwd: Option<String>,
    #[builder(default, setter(strip_option))]
    pub env: Option<HashMap<String, P>>,
    #[builder(default, setter(strip_option))]
    pub stdout: Option<P>,
    #[builder(default, setter(strip_option))]
    pub stderr: Option<P>,
}

pub type RunRequest = RunSpecification<Param>;
pub(crate) type RunRecipe = RunSpecification<String>;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RunResponse {
    pub return_code: i32,
    pub exc: Option<String>,
}
