use std::{
    fs::File,
    path::PathBuf,
    process::{ExitStatus, Stdio},
};

use lazy_static::lazy_static;
use log::debug;
use mongodb::bson::oid::ObjectId;
use mongodb_gridfs::GridFSBucket;
use mongodb_gridfs_ext::bucket::file_sync::FileSync;
use regex::{Captures, Regex};
use tempfile::{tempdir, NamedTempFile, TempDir, TempPath};

use crate::protocol::RunRequest;

lazy_static! {
    pub static ref LINK_REGEX: Regex = Regex::new(r"<#:([io])>(.+?)</>").unwrap();
}

#[derive(Debug)]
pub struct RunContext {
    workspace: TempDir,
    bucket: GridFSBucket,
    pub(crate) args: Vec<String>,
    original_args: Vec<String>,
    stdout: Option<PathBuf>,
    stderr: Option<PathBuf>,
    downloaded: Vec<(TempPath, String)>,
    to_uploads: Vec<(TempPath, String)>,
}

impl RunContext {
    pub(crate) async fn new(req: RunRequest, bucket: GridFSBucket) -> Self {
        let workspace = tempdir().expect("Failed to create temp workspace.");
        let stdout = req.stdout.map(|stdout_link| {
            (
                tempfile::Builder::new()
                    .prefix("stdout")
                    .tempfile_in(workspace.path())
                    .unwrap()
                    .into_temp_path(),
                stdout_link,
            )
        });
        let stderr = req.stderr.map(|stderr_link| {
            (
                tempfile::Builder::new()
                    .prefix("stderr")
                    .tempfile_in(workspace.path())
                    .unwrap()
                    .into_temp_path(),
                stderr_link,
            )
        });
        let mut ctx = RunContext {
            workspace,
            bucket,
            args: vec![],
            original_args: vec![],
            stdout: stdout.as_ref().map(|(tp, _)| tp.to_path_buf()),
            stderr: stderr.as_ref().map(|(tp, _)| tp.to_path_buf()),
            downloaded: req
                .to_downloads
                .unwrap_or_default()
                .into_iter()
                .map(|(local_path, uri)| (RunContext::resolve_relpath(local_path), uri))
                .map(|(local_path, uri)| (TempPath::from_path(local_path), uri))
                .collect(),
            to_uploads: req
                .to_uploads
                .unwrap_or_default()
                .into_iter()
                .map(|(local_path, uri)| (RunContext::resolve_relpath(local_path), uri))
                .map(|(local_path, uri)| (TempPath::from_path(local_path), uri))
                .chain(stdout.into_iter())
                .chain(stderr.into_iter())
                .collect(),
        };

        ctx.args = req.args.iter().map(|arg| ctx.resolve_opt(arg)).collect();
        ctx.original_args = req.args;

        ctx.download().await.into_iter().for_each(|res| {
            res.unwrap();
        });
        ctx
    }

    pub fn call(&self, program: &PathBuf) -> std::io::Result<ExitStatus> {
        let stdout = self
            .stdout
            .as_ref()
            .map(|path| Stdio::from(File::create(path).unwrap()))
            .unwrap_or_else(Stdio::inherit);
        let stderr = self
            .stderr
            .as_ref()
            .map(|path| Stdio::from(File::create(path).unwrap()))
            .unwrap_or_else(Stdio::inherit);

        let mut command = std::process::Command::new(program);
        let command = command.args(&self.args);
        command.stdout(stdout).stderr(stderr).status()
    }

    /// Replace env var in the path with the real path.
    fn resolve_relpath(path: String) -> String {
        // todo!("replace the env var, i.e. workspace, in the given path string.")
        path
    }

    /// Parse cloud uris in forms of '<#:\[io\]></>', and bind each of them with a local temp path.
    fn resolve_opt(&mut self, opt: &str) -> String {
        LINK_REGEX
            .replace_all(opt, |caps: &Captures| {
                let is_input = caps.get(1).unwrap().as_str() == "i";
                let uri = caps.get(2).unwrap().as_str().to_string();

                let local_path = self.temppath();
                let local_path_string = local_path.to_str().unwrap().to_string();

                if is_input {
                    self.downloaded.push((local_path, uri))
                } else {
                    self.to_uploads.push((local_path, uri))
                }
                local_path_string
            })
            .to_string()
    }

    /// Create an instance of TempPath in the workspace.
    fn temppath(&self) -> TempPath {
        NamedTempFile::new_in(self.workspace.as_ref())
            .expect("Failed to create temp file.")
            .into_temp_path()
    }

    /// Download cloud files to their binding local paths
    async fn download(&self) -> Vec<mongodb_gridfs_ext::error::Result<ObjectId>> {
        futures::future::join_all(self.downloaded.iter().map(|(local_path, uri)| async {
            let local_path = local_path.to_str().unwrap().to_string();
            let uri = uri.clone();
            debug!(
                "try downloading `{}' from cloud to local `{}'",
                uri, local_path
            );
            // download from the link to the local_path
            self.bucket.download_to(&uri, &local_path).await
        }))
        .await
    }

    /// Upload local paths to their binding cloud uri
    async fn upload(&self) -> Vec<mongodb_gridfs_ext::error::Result<ObjectId>> {
        futures::future::join_all(self.to_uploads.iter().map(|(local_path, uri)| async {
            let mut bucket = self.bucket.clone();
            let local_path = local_path.to_str().unwrap().to_string();
            let uri = uri.clone();
            bucket.upload_from(&uri, &local_path).await
        }))
        .await
    }
}

impl Drop for RunContext {
    fn drop(&mut self) {
        futures::executor::block_on(self.upload())
            .into_iter()
            .for_each(|res| {
                res.unwrap();
            });
    }
}

#[cfg(test)]
mod tests {
    use chain_ext::mongodb_gridfs::DatabaseExt;
    use fake::Fake;
    use futures::stream::StreamExt;
    use mongodb::Client;
    use rand::prelude::IteratorRandom;
    use test_utilities::docker::Builder as ContainerBuilder;
    use test_utilities::{fs, gridfs};

    use super::*;

    #[tokio::test]
    async fn test_run_context_without_remote_in_out_files() {
        let mongo_handle = ContainerBuilder::new("mongo")
            .port_mapping(0, Some(27017))
            .build_disposable()
            .await;

        let args = vec!["args", "-that", "contains", "-no", "remote", "links"]
            .iter()
            .map(|s| s.to_string())
            .collect::<Vec<String>>();

        let req = RunRequest {
            command: "sh".to_owned(),
            args: args.clone(),
            ..RunRequest::default()
        };

        let bucket = Client::with_uri_str(mongo_handle.url.as_ref().unwrap())
            .await
            .unwrap()
            .database("testdb")
            .bucket(None);

        let ctx = RunContext::new(req, bucket).await;

        assert!(ctx.downloaded.is_empty());
        assert!(ctx.to_uploads.is_empty());
        assert_eq!(ctx.args, args);
        assert_eq!(ctx.original_args, args);
    }

    #[tokio::test]
    async fn test_run_context_with_remote_in_files() {
        wait_for_avoiding_concurrent_docker().await;
        let mongo_handle = ContainerBuilder::new("mongo")
            .port_mapping(0, Some(27017))
            .build_disposable()
            .await;

        let mut bucket = Client::with_uri_str(mongo_handle.url.as_ref().unwrap())
            .await
            .unwrap()
            .database("testdb")
            .bucket(None);
        let (_oid, link1, random_doc1) = prepare_cloud_file(&mut bucket).await;
        let (_oid, link2, random_doc2) = prepare_cloud_file(&mut bucket).await;

        let args: Vec<String> = vec![
            "-input".into(),
            format!("<#:i>{}</>", link1),
            format!("-S<#:i>{}</>", link2),
            "mongo".into(),
        ];

        let req = RunRequest {
            command: "sh".to_owned(),
            args: args.clone(),
            ..RunRequest::default()
        };

        let workspace: Box<std::path::Path>;
        {
            let ctx = RunContext::new(req, bucket).await;
            workspace = ctx.workspace.path().to_path_buf().into_boxed_path();

            assert_eq!(ctx.downloaded.len(), 2);
            assert_eq!(ctx.to_uploads.len(), 0);
            assert_eq!(ctx.original_args, args);

            // assert the ordinary args are preserved
            assert_eq!(ctx.args[0], args[0]);
            assert_eq!(ctx.args[3], args[3]);

            let local_download_path1 = ctx.downloaded[0].0.to_str().unwrap();
            let local_download_path2 = ctx.downloaded[1].0.to_str().unwrap();

            // assert the links in args have been replaced by local paths
            assert_eq!(ctx.args[1], format!("{}", local_download_path1));
            assert_eq!(ctx.args[2], format!("-S{}", local_download_path2));

            let download_doc1 = tokio::fs::read_to_string(local_download_path1)
                .await
                .unwrap();
            let download_doc2 = tokio::fs::read_to_string(local_download_path2)
                .await
                .unwrap();

            // assert the links have been downloaded correctly
            assert_eq!(download_doc1.into_bytes(), random_doc1);
            assert_eq!(download_doc2.into_bytes(), random_doc2);
        }

        // assert the workspace has been removed
        assert!(!workspace.exists())
    }

    #[tokio::test]
    async fn test_run_context_with_remote_out_files() {
        wait_for_avoiding_concurrent_docker().await;
        let mongo_handle = ContainerBuilder::new("mongo")
            .port_mapping(0, Some(27017))
            .build_disposable()
            .await;

        let mut bucket = Client::with_uri_str(mongo_handle.url.as_ref().unwrap())
            .await
            .unwrap()
            .database("testdb")
            .bucket(None);
        let (oid1, link1, random_doc1) = prepare_cloud_file(&mut bucket).await;
        let (oid2, link2, random_doc2) = prepare_cloud_file(&mut bucket).await;

        let args: Vec<String> = vec![
            "-output".into(),
            format!("<#:o>{}</>", link1),
            format!("-O<#:o>{}</>", link2),
            "mongo".into(),
        ];

        let req = RunRequest {
            command: "sh".to_owned(),
            args: args.clone(),
            ..RunRequest::default()
        };

        let workspace: Box<std::path::Path>;
        {
            let ctx = RunContext::new(req, bucket.clone()).await;
            workspace = ctx.workspace.path().to_path_buf().into_boxed_path();

            assert_eq!(ctx.downloaded.len(), 0);
            assert_eq!(ctx.to_uploads.len(), 2);
            assert_eq!(ctx.original_args, args);

            // assert the ordinary args are preserved
            assert_eq!(ctx.args[0], args[0]);
            assert_eq!(ctx.args[3], args[3]);

            let local_to_upload_path1 = ctx.to_uploads[0].0.to_str().unwrap();
            let local_to_upload_path2 = ctx.to_uploads[1].0.to_str().unwrap();

            // assert the links in args have been replaced by local paths
            assert_eq!(ctx.args[1], format!("{}", local_to_upload_path1));
            assert_eq!(ctx.args[2], format!("-O{}", local_to_upload_path2));
        }

        let uploaded_doc1: Vec<u8> = bucket
            .open_download_stream(oid1)
            .await
            .unwrap()
            .next()
            .await
            .unwrap();
        let uploaded_doc2: Vec<u8> = bucket
            .open_download_stream(oid2)
            .await
            .unwrap()
            .next()
            .await
            .unwrap();

        assert_eq!(uploaded_doc1, random_doc1);
        assert_eq!(uploaded_doc2, random_doc2);

        assert!(!workspace.exists());
    }

    #[tokio::test]
    async fn test_run_context_with_remote_files() {
        wait_for_avoiding_concurrent_docker().await;
        let mongo_handle = ContainerBuilder::new("mongo")
            .port_mapping(0, Some(27017))
            .build_disposable()
            .await;

        let mut bucket = Client::with_uri_str(mongo_handle.url.as_ref().unwrap())
            .await
            .unwrap()
            .database("testdb")
            .bucket(None);
        let (_oid, link1, random_doc1) = prepare_cloud_file(&mut bucket).await;
        let (oid2, link2, random_doc2) = prepare_cloud_file(&mut bucket).await;

        let (stdout_link, stderr_link) = ("stdout-link", "stderr-link");

        let args: Vec<String> = vec![
            "-output".into(),
            format!("cat <#:i>{}</> > <#:o>{}</>", link1, link2),
            "mongo".into(),
        ];

        let req = RunRequest {
            command: "sh".to_owned(),
            args: args.clone(),
            stdout: Some(stdout_link.to_owned()),
            stderr: Some(stderr_link.to_owned()),
            ..RunRequest::default()
        };

        let workspace: Box<std::path::Path>;
        {
            let ctx = RunContext::new(req, bucket.clone()).await;
            workspace = ctx.workspace.path().to_path_buf().into_boxed_path();

            assert_eq!(ctx.downloaded.len(), 1);
            assert_eq!(ctx.to_uploads.len(), 3);
            assert_eq!(ctx.original_args, args);

            // assert the ordinary args are preserved
            assert_eq!(ctx.args[0], args[0]);
            assert_eq!(ctx.args[2], args[2]);

            let local_download_path1 = ctx.downloaded[0].0.to_str().unwrap();
            let local_to_upload_path2 = ctx.to_uploads[2].0.to_str().unwrap();

            // assert the links in args have been replaced by local paths
            assert_eq!(
                ctx.args[1],
                format!("cat {} > {}", local_download_path1, local_to_upload_path2)
            );

            let local_to_upload_stdout_path = ctx.to_uploads[0].0.to_str().unwrap();
            let local_to_upload_stderr_path = ctx.to_uploads[1].0.to_str().unwrap();

            // assert both stdout and stderr have been bound to local paths, and waiting for upload
            assert_eq!(
                local_to_upload_stdout_path,
                ctx.stdout.as_ref().unwrap().to_str().unwrap()
            );
            assert_eq!(
                local_to_upload_stderr_path,
                ctx.stderr.as_ref().unwrap().to_str().unwrap()
            );

            let download_doc1 = tokio::fs::read(local_download_path1).await.unwrap();
            assert_eq!(download_doc1, random_doc1);
        }

        let uploaded_doc2 = bucket
            .open_download_stream(oid2)
            .await
            .unwrap()
            .next()
            .await
            .unwrap();
        assert_eq!(uploaded_doc2, random_doc2);

        assert!(!workspace.exists());
    }

    async fn prepare_cloud_file(bucket: &mut GridFSBucket) -> (ObjectId, String, Vec<u8>) {
        let faker = gridfs::TempFileFaker::with_bucket(bucket.clone())
            .kind(fs::TempFileKind::Text)
            .include_content(true);
        let temp_file = faker.fake::<gridfs::TempFile>();

        (
            temp_file.id,
            temp_file.filename.unwrap(),
            temp_file.content.unwrap(),
        )
    }

    async fn wait_for_avoiding_concurrent_docker() {
        let random_gap =
            tokio::time::Duration::from_millis((10..1000).choose(&mut rand::thread_rng()).unwrap());
        tokio::time::sleep(random_gap).await;
    }
}
