#![allow(non_upper_case_globals)]

use std::path::PathBuf;

use anyhow::Result;
use celery::prelude::*;
use chain_ext::option::OptionExt;
use clap::Parser;
use directories::UserDirs;

use crate::configs::{CmdProxyServerConf, CmdProxyServerConfFile};
use crate::tasks::{run, SERVER_CONF};

pub mod middles;
pub mod params;
pub mod protocol;
// mod run_context;
pub mod client;
mod codegen;
pub mod configs;
mod server;
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

    let redis_url = cli
        .redis_uri
        .or_ok(std::env::var("REDIS_URI"))
        .or_wrap("redis://localhost:6379/".into())
        .unwrap();

    let mongo_url = cli
        .mongo_uri
        .or_ok(std::env::var("MONGO_URI"))
        .or_wrap("mongodb://localhost:27017/".into())
        .unwrap();

    let mongodb_name = cli
        .mongodb_name
        .or_ok(std::env::var("MONGODB_NAME"))
        .or_wrap("testdb".to_owned())
        .unwrap();

    let command_palette = cli
        .command_palette_path
        .or_ok(std::env::var("COMMANDS_PALETTE").map(PathBuf::from))
        .or_else(|| {
            let default_path =
                UserDirs::new().map(|dirs| dirs.home_dir().join("commands-palette.yaml"));
            if default_path.is_some() && default_path.as_ref().unwrap().exists() {
                default_path
            } else {
                None
            }
        });

    SERVER_CONF
        .set(CmdProxyServerConf::new(CmdProxyServerConfFile {
            redis_url,
            mongo_url,
            mongodb_name,
            command_palette,
        }))
        .unwrap();

    let app = celery::app!(
        broker = RedisBroker { SERVER_CONF.get().unwrap().celery.broker_url },
        backend = MongoDbBackend { SERVER_CONF.get().unwrap().celery.backend_url },
        tasks = [run],
        task_routes = [
            // this bin will only run in server mode, hence no task needs to be routed
            // "*" => "proxy-queue",
        ],
    )
    .await?;

    let command_queues: Vec<_> = SERVER_CONF
        .get()
        .unwrap()
        .command_palette
        .as_ref()
        .map(|palette| palette.keys().map(|k| k.as_str()).collect())
        .unwrap_or_default();

    app.display_pretty().await;
    app.consume_from(command_queues.as_slice()).await?;

    Ok(())
}
