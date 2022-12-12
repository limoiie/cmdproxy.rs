use std::collections::HashMap;
use std::path::Path;

use mongodb::bson::oid::ObjectId;
use mongodb_gridfs::GridFSBucket;
use mongodb_gridfs_ext::bucket::common::GridFSBucketExt;
use mongodb_gridfs_ext::bucket::file_sync::FileSync;
use mongodb_gridfs_ext::error::Result as GridFSExtResult;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Param {
    StrParam {
        value: String,
    },
    EnvParam {
        name: String,
    },
    RemoteEnvParam {
        name: String,
    },
    InLocalFileParam {
        filepath: String,
        hostname: String,
    },
    OutLocalFileParam {
        filepath: String,
        hostname: String,
    },
    InCloudFileParam {
        filepath: String,
        hostname: String,
    },
    OutCloudFileParam {
        filepath: String,
        hostname: String,
    },
    FormatParam {
        tmpl: String,
        args: HashMap<String, Param>,
    },
}

impl Param {
    pub fn str<S: AsRef<str>>(value: S) -> Param {
        Param::StrParam {
            value: value.as_ref().to_string(),
        }
    }

    pub fn ipath<S: AsRef<str>>(filepath: S) -> Param {
        let filepath = filepath.as_ref().to_string();
        let hostname = hostname::get().unwrap().into_string().unwrap();
        Param::InLocalFileParam { filepath, hostname }
    }

    pub fn opath<S: AsRef<str>>(filepath: S) -> Param {
        let filepath = filepath.as_ref().to_string();
        let hostname = hostname::get().unwrap().into_string().unwrap();
        Param::OutLocalFileParam { filepath, hostname }
    }

    pub fn env<S: AsRef<str>>(name: S) -> Param {
        Param::EnvParam {
            name: name.as_ref().to_string(),
        }
    }

    pub fn remote_env<S: AsRef<str>>(name: S) -> Param {
        Param::RemoteEnvParam {
            name: name.as_ref().to_string(),
        }
    }

    pub fn format<S: AsRef<str>>(tmpl: S, args: HashMap<&str, Param>) -> Param {
        Param::FormatParam {
            tmpl: tmpl.as_ref().to_string(),
            args: args
                .into_iter()
                .map(|(key, param)| (key.to_string(), param))
                .collect(),
        }
    }

    pub fn hostname(&self) -> &str {
        match self {
            Param::InLocalFileParam { hostname, .. } => hostname,
            Param::OutLocalFileParam { hostname, .. } => hostname,
            Param::InCloudFileParam { hostname, .. } => hostname,
            Param::OutCloudFileParam { hostname, .. } => hostname,
            _ => unreachable!(),
        }
    }

    pub fn filepath(&self) -> &str {
        match self {
            Param::InLocalFileParam { filepath, .. } => filepath,
            Param::OutLocalFileParam { filepath, .. } => filepath,
            Param::InCloudFileParam { filepath, .. } => filepath,
            Param::OutCloudFileParam { filepath, .. } => filepath,
            _ => unreachable!(),
        }
    }

    pub fn is_input(&self) -> bool {
        matches!(
            self,
            Param::InLocalFileParam { .. } | Param::InCloudFileParam { .. }
        )
    }

    pub fn is_output(&self) -> bool {
        matches!(
            self,
            Param::OutLocalFileParam { .. } | Param::OutCloudFileParam { .. }
        )
    }

    pub fn is_local(&self) -> bool {
        matches!(
            self,
            Param::InLocalFileParam { .. } | Param::OutLocalFileParam { .. }
        )
    }

    pub fn is_cloud(&self) -> bool {
        matches!(
            self,
            Param::InCloudFileParam { .. } | Param::OutCloudFileParam { .. }
        )
    }

    pub fn as_cloud(&self) -> Param {
        match self.clone() {
            Param::InLocalFileParam { filepath, hostname } => {
                Param::InCloudFileParam { filepath, hostname }
            }
            Param::OutLocalFileParam { filepath, hostname } => {
                Param::OutCloudFileParam { filepath, hostname }
            }
            cloud @ Param::InCloudFileParam { .. } => cloud,
            cloud @ Param::OutCloudFileParam { .. } => cloud,
            _ => unreachable!(),
        }
    }

    pub fn cloud_url(&self) -> String {
        format!(
            "@{hostname}:{filepath}",
            hostname = self.hostname(),
            filepath = self.filepath()
        )
    }

    pub async fn id_on_cloud(&self, bucket: GridFSBucket) -> GridFSExtResult<ObjectId> {
        bucket.id(self.cloud_url().as_str()).await
    }

    pub async fn exists_on_cloud(&self, bucket: GridFSBucket) -> GridFSExtResult<bool> {
        bucket.exists(self.cloud_url().as_str()).await
    }

    pub async fn remove_from_cloud(&self, bucket: GridFSBucket) -> GridFSExtResult<()> {
        bucket
            .delete(self.id_on_cloud(bucket.clone()).await?)
            .await
            .map_err(Into::into)
    }

    pub async fn download(
        &self,
        bucket: GridFSBucket,
        filepath: impl AsRef<Path> + Send + Sync,
    ) -> GridFSExtResult<ObjectId> {
        bucket
            .download_to(self.cloud_url().as_str(), filepath)
            .await
    }

    pub async fn upload(
        &self,
        mut bucket: GridFSBucket,
        filepath: impl AsRef<Path> + Send,
    ) -> GridFSExtResult<ObjectId> {
        bucket
            .upload_from(self.cloud_url().as_str(), filepath)
            .await
    }

    pub async fn download_inplace(&self, bucket: GridFSBucket) -> GridFSExtResult<ObjectId> {
        assert!(self.is_local());
        self.download(bucket, self.filepath()).await
    }

    pub async fn upload_inplace(&self, bucket: GridFSBucket) -> GridFSExtResult<ObjectId> {
        assert!(self.is_local());
        self.upload(bucket, self.filepath()).await
    }
}
