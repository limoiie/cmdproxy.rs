use celery::export::async_trait;

use crate::middles::Middle;
use crate::protocol::{RunRequest, RunResponse};

pub struct MiddleImpl {}

impl MiddleImpl {
    pub fn new() -> MiddleImpl {
        MiddleImpl {}
    }
}

#[async_trait]
impl Middle<String, String, RunRequest, RunResponse> for MiddleImpl {
    async fn transform_request(&self, request: String) -> anyhow::Result<RunRequest> {
        Ok(serde_json::from_str(request.as_str())?)
    }

    async fn transform_response(
        &self,
        response: anyhow::Result<RunResponse>,
    ) -> anyhow::Result<String> {
        let response = match response {
            Ok(response) => response,
            Err(err) => RunResponse {
                return_code: -1,
                exc: Some(err.to_string()),
            },
        };
        Ok(serde_json::to_string(&response)?)
    }
}
