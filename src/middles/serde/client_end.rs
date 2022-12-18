use celery::export::async_trait;

use crate::middles::Middle;
use crate::protocol::{RunRequest, RunResponse};

pub(crate) struct MiddleImpl {}

impl MiddleImpl {
    pub(crate) fn new() -> MiddleImpl {
        MiddleImpl {}
    }
}

#[async_trait]
impl Middle<RunRequest, RunResponse, String, String> for MiddleImpl {
    async fn transform_request(&self, request: RunRequest) -> anyhow::Result<String> {
        Ok(serde_json::to_string(&request)?)
    }

    async fn transform_response(
        &self,
        response: anyhow::Result<String>,
    ) -> anyhow::Result<RunResponse> {
        let response: RunResponse = serde_json::from_str(response?.as_str())?;
        if response.exc.is_some() {
            anyhow::bail!(
                "Server Error: return code {}, {}",
                response.return_code,
                response.exc.unwrap()
            )
        } else {
            Ok(response)
        }
    }
}
