use std::collections::HashMap;
use std::path::PathBuf;

use celery::prelude::TaskResult;
use celery::task::TaskResultExt;
use mongodb::{bson::doc, Client};
use mongodb_gridfs::GridFSBucket;
use once_cell::sync::OnceCell;

use crate::run_context::RunContext;
use crate::run_request::RunRequest;

pub static REDIS_URI: OnceCell<String> = OnceCell::new();
pub static MONGO_URI: OnceCell<String> = OnceCell::new();

pub static CLIENT: OnceCell<Client> = OnceCell::new();
pub static BUCKET: OnceCell<GridFSBucket> = OnceCell::new();

pub static COMMANDS_MAP: OnceCell<HashMap<String, PathBuf>> = OnceCell::new();

#[celery::task]
pub async fn run(serialized_run_request: String) -> TaskResult<i32> {
    let run_request =
        serde_json::from_str::<RunRequest>(&serialized_run_request).unwrap_or_else(|err| {
            log::error!("Failed to deserialize request: {}", serialized_run_request);
            panic!("Failed due to {}", err)
        });

    let program_path = COMMANDS_MAP
        .get()
        .unwrap()
        .get(&run_request.command)
        .expect(format!("Missing command {}", run_request.command).as_str());

    RunContext::new(run_request, BUCKET.get().unwrap().clone())
        .await
        .call(program_path)
        .map(|es| es.code().unwrap())
        .with_unexpected_err(|| "")
}
