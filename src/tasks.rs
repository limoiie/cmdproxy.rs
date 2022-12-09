use celery::error::TaskError::UnexpectedError;
use std::collections::HashMap;
use std::path::PathBuf;

use celery::prelude::TaskResult;
use celery::task::TaskResultExt;
use mongodb::{bson::doc, Client};
use mongodb_gridfs::GridFSBucket;
use once_cell::sync::OnceCell;

use crate::protocol::RunRequest;
use crate::run_context::RunContext;

pub static REDIS_URI: OnceCell<String> = OnceCell::new();
pub static MONGO_URI: OnceCell<String> = OnceCell::new();

pub static CLIENT: OnceCell<Client> = OnceCell::new();
pub static BUCKET: OnceCell<GridFSBucket> = OnceCell::new();

pub static COMMANDS_PALETTE: OnceCell<HashMap<String, PathBuf>> = OnceCell::new();

#[celery::task]
pub async fn run(serialized_run_request: String) -> TaskResult<i32> {
    let run_request = serde_json::from_str::<RunRequest>(&serialized_run_request)
        .with_unexpected_err(|| {
            format!("Failed to deserialize Request {}", serialized_run_request)
        })?;

    let program_path = COMMANDS_PALETTE
        .get()
        .unwrap()
        .get(&run_request.command)
        .ok_or_else(|| {
            let valid_commands = COMMANDS_PALETTE.get().unwrap().keys().collect::<Vec<_>>();
            UnexpectedError(format!(
                "Failed to get path for command {},\n  valid commands include: {:#?}",
                run_request.command, valid_commands
            ))
        })?;

    RunContext::new(run_request, BUCKET.get().unwrap().clone())
        .await
        .call(program_path)
        .map(|es| es.code().unwrap())
        .with_unexpected_err(|| format!("Failed to run the command at {:#?}", program_path))
}
