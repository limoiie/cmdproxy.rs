use serde::{Deserialize, Serialize};

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct RunRequest {
    pub command: String,
    pub args: Vec<String>,
    pub to_downloads: Option<Vec<(String, String)>>,
    pub to_uploads: Option<Vec<(String, String)>>,
    pub stdout: Option<String>,
    pub stderr: Option<String>,
}
