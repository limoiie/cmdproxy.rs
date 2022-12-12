use std::collections::HashMap;
use std::path::PathBuf;

use chain_ext::io::DeExt;
use chain_ext::mongodb_gridfs::DatabaseExt;
use mongodb_gridfs::GridFSBucket;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug)]
pub struct CeleryConf {
    pub broker_url: String,
    pub backend_url: String,
}

#[derive(Clone, Debug)]
pub struct CloudFSConf {
    pub mongodb_url: String,
    pub mongodb_name: String,
}

impl CloudFSConf {
    pub(crate) async fn client(&self) -> mongodb::Client {
        mongodb::Client::with_uri_str(self.mongodb_url.as_str())
            .await
            .unwrap()
    }

    pub(crate) async fn db(&self) -> mongodb::Database {
        self.client().await.database(self.mongodb_name.as_str())
    }

    pub(crate) async fn grid_fs(&self) -> GridFSBucket {
        self.db().await.bucket(None)
    }
}

#[derive(Debug, Default, Serialize, Deserialize)]
pub struct CmdProxyClientConfFile {
    pub redis_url: String,
    pub mongo_url: String,
    pub mongodb_name: String,
}

#[derive(Debug, Default, Serialize, Deserialize)]
pub struct CmdProxyServerConfFile {
    pub redis_url: String,
    pub mongo_url: String,
    pub mongodb_name: String,
    pub command_palette: Option<PathBuf>,
}

pub struct CmdProxyClientConf {
    pub celery: CeleryConf,
    pub cloud: CloudFSConf,
}

impl CmdProxyClientConf {
    pub fn new(conf: CmdProxyClientConfFile) -> CmdProxyClientConf {
        CmdProxyClientConf {
            celery: CeleryConf {
                broker_url: conf.redis_url,
                backend_url: conf.mongo_url.clone(),
            },
            cloud: CloudFSConf {
                mongodb_url: conf.mongo_url,
                mongodb_name: conf.mongodb_name,
            },
        }
    }
}

#[derive(Clone, Debug)]
pub struct CmdProxyServerConf {
    pub(crate) celery: CeleryConf,
    pub(crate) cloud: CloudFSConf,
    pub command_palette: HashMap<String, String>,
    pub command_palette_path: Option<PathBuf>,
}

impl CmdProxyServerConf {
    pub fn new(conf: CmdProxyServerConfFile) -> CmdProxyServerConf {
        let command_palette = conf
            .command_palette
            .as_ref()
            .and_then(|p| {
                if p.exists() {
                    Some(
                        std::fs::read_to_string(p)
                            .unwrap()
                            .as_bytes()
                            .de_yaml()
                            .unwrap(),
                    )
                } else {
                    None
                }
            })
            .unwrap_or_default();

        CmdProxyServerConf {
            celery: CeleryConf {
                broker_url: conf.redis_url,
                backend_url: conf.mongo_url.clone(),
            },
            cloud: CloudFSConf {
                mongodb_url: conf.mongo_url,
                mongodb_name: conf.mongodb_name,
            },
            command_palette,
            command_palette_path: conf.command_palette,
        }
    }
}
