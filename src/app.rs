use std::collections::HashMap;
use std::path::PathBuf;

use celery::prelude::*;
use chain_ext::io::DeExt;
use chain_ext::option::OptionExt;
use clap::Parser;
use directories::UserDirs;

use crate::configs::{CmdProxyServerConf, CmdProxyServerConfFile};
use crate::tasks::{run, SERVER_CONF};

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
pub struct Cli {
    /// Uri to the redis broker
    #[arg(short, long)]
    redis_url: Option<String>,

    /// Uri to the mongo remote-fs
    #[arg(short, long)]
    mongo_url: Option<String>,

    /// Name of database where stores the remote-fs
    #[arg(long)]
    mongodb_name: Option<String>,

    /// Log level
    #[arg(short, long)]
    loglevel: Option<String>,

    /// Path to a command palette file mapping program name to their paths
    #[arg(short, long)]
    command_palette: Option<PathBuf>,

    /// Path to a environment file
    #[arg(short, long)]
    environments: Option<PathBuf>,

    /// Extension queues separated by comma.
    #[arg(short, long)]
    ext_queues: Option<String>,
}

pub async fn app(cli: Cli) -> anyhow::Result<()> {
    env_logger::Builder::new()
        .parse_filters(
            cli.loglevel
                .or_ok(std::env::var("CMDPROXY_LOGLEVEL"))
                .or_wrap("info".into())
                .unwrap()
                .as_str(),
        )
        .init();

    let redis_url = cli
        .redis_url
        .or_ok(std::env::var("CMDPROXY_REDIS_URL"))
        .or_wrap("redis://localhost:6379/".into())
        .unwrap();

    let mongo_url = cli
        .mongo_url
        .or_ok(std::env::var("CMDPROXY_MONGO_URL"))
        .or_wrap("mongodb://localhost:27017/".into())
        .unwrap();

    let mongodb_name = cli
        .mongodb_name
        .or_ok(std::env::var("CMDPROXY_MONGODB_NAME"))
        .or_wrap("testdb".to_owned())
        .unwrap();

    let command_palette = cli
        .command_palette
        .or_ok(std::env::var("CMDPROXY_COMMAND_PALETTE").map(PathBuf::from))
        .or_else(|| {
            UserDirs::new().map(|dirs| {
                dirs.home_dir()
                    .join(".cmdproxy")
                    .join("commands-palette.yaml")
            })
        });

    let ext_queues = cli
        .ext_queues
        .or_ok(std::env::var("CMDPROXY_EXT_QUEUES"))
        .unwrap_or_default();

    SERVER_CONF
        .set(CmdProxyServerConf::new(CmdProxyServerConfFile {
            redis_url,
            mongo_url,
            mongodb_name,
            command_palette,
        }))
        .unwrap();

    cli.environments
        .or_ok(std::env::var("CMDPROXY_ENVIRONMENTS").map(PathBuf::from))
        .or_else(|| {
            UserDirs::new().map(|dirs| dirs.home_dir().join(".cmdproxy").join("environments.yaml"))
        })
        .map(|environments| {
            if environments.exists() {
                std::fs::read_to_string(environments)
                    .unwrap()
                    .as_bytes()
                    .de_yaml::<HashMap<String, String>>()
                    .unwrap()
                    .iter()
                    .chain(
                        // also insert command palette into environment variables, so that we can
                        // resolve command path via EnvParam
                        SERVER_CONF.get().unwrap().command_palette.iter(),
                    )
                    .for_each(|(key, val)| std::env::set_var(key, val));
            }
        })
        .unwrap_or_default();

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
        .keys()
        .map(String::as_str)
        .chain(ext_queues.split(','))
        .filter(|queue| !queue.is_empty())
        .collect();
    assert!(!command_queues.is_empty(), "No queues to be consumed!");

    app.display_pretty().await;
    app.consume_from(command_queues.as_slice()).await?;

    Ok(())
}
