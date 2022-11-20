#![allow(non_upper_case_globals)]

use std::path::PathBuf;

use anyhow::Result;
use celery::prelude::*;
use chain_ext::io::DeExt;
use chain_ext::mongodb_gridfs::DatabaseExt;
use chain_ext::option::OptionExt;
use chain_ext::path::file_ext::FileExt;
use clap::Parser;
use directories::UserDirs;
use mongodb::Client;

use crate::tasks::{run, BUCKET, CLIENT, COMMANDS_MAP, MONGO_URI, REDIS_URI};

mod run_context;
pub mod run_request;
pub mod tasks;

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
pub struct Cli {
    /// Uri to the redis broker
    #[arg(short, long)]
    redis_uri: Option<String>,

    /// Uri to the mongo remote-fs
    #[arg(short, long)]
    mongo_uri: Option<String>,

    /// Name of database where stores the remote-fs
    #[arg(long)]
    mongodb_name: Option<String>,

    /// Log level
    #[arg(short, long)]
    log: Option<String>,

    ///
    #[arg(short, long)]
    command_palette_path: Option<PathBuf>,
}

pub async fn app(cli: Cli) -> Result<()> {
    env_logger::Builder::new()
        .parse_filters(
            cli.log
                .or_ok(std::env::var("RUST_LOG"))
                .or_wrap("info".into())
                .unwrap()
                .as_str(),
        )
        .init();

    REDIS_URI
        .set(
            cli.redis_uri
                .or_ok(std::env::var("REDIS_URI"))
                .or_wrap("redis://localhost:6379/".into())
                .unwrap(),
        )
        .unwrap();

    MONGO_URI
        .set(
            cli.mongo_uri
                .or_ok(std::env::var("MONGO_URI"))
                .or_wrap("mongodb://localhost:27017/".into())
                .unwrap(),
        )
        .unwrap();

    CLIENT
        .set(
            Client::with_uri_str(MONGO_URI.get().unwrap())
                .await
                .unwrap(),
        )
        .unwrap();

    BUCKET
        .set(
            cli.mongodb_name
                .or_ok(std::env::var("MONGODB_NAME"))
                .or_wrap("testdb".to_owned())
                .map(|name| CLIENT.get().unwrap().database(&name))
                .or_else(|| CLIENT.get().unwrap().default_database())
                .expect("Failed to connect to default Mongodb Database")
                .bucket(None),
        )
        .unwrap();

    COMMANDS_MAP
        .set(
            cli.command_palette_path
                .or_ok(std::env::var("COMMANDS_PALETTE").map(PathBuf::from))
                .or_else(|| {
                    UserDirs::new().map(|dirs| dirs.home_dir().join("commands-palette.yaml"))
                })
                .expect("Commands palette file not found")
                .open()
                .unwrap()
                .de_yaml()
                .unwrap(),
        )
        .unwrap();

    let app = celery::app!(
        broker = RedisBroker { REDIS_URI.get().unwrap() },
        backend = RedisBackend { REDIS_URI.get().unwrap() },
        tasks = [run],
        task_routes = [
            // this bin will only run in server mode, hence no task needs to be routed
            // "*" => "proxy-queue",
        ],
    )
    .await?;

    let command_queues = COMMANDS_MAP
        .get()
        .unwrap()
        .keys()
        .map(|k| k.as_str())
        .collect::<Vec<_>>();

    app.display_pretty().await;
    app.consume_from(command_queues.as_slice()).await?;

    Ok(())
}
