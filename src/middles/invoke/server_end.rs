use std::cell::RefCell;
use std::collections::HashMap;
use std::sync::Arc;

use celery::export::async_trait;
use mongodb_gridfs::GridFSBucket;
use strfmt::strfmt;
use tempfile::{TempDir, TempPath};
use tokio::sync::Mutex;

use crate::middles::invoke::{
    guard_hashmap_args, guard_run_args, ArcMtxRefCell, ArgGuard, GuardStack,
};
use crate::middles::Middle;
use crate::params::Param;
use crate::protocol::{RunRecipe, RunRequest, RunResponse};

struct StrGuard {
    value: String,
}

struct EnvGuard {
    name: String,
}

struct InCloudFileGuard {
    temppath: TempPath,
    bucket: GridFSBucket,
    param: Param,
}

struct OutCloudFileGuard {
    temppath: TempPath,
    bucket: GridFSBucket,
    param: Param,
}

struct FormatGuard {
    tmpl: String,
    args: HashMap<String, Param>,
    ctx: ContextStack,
}

#[async_trait]
impl ArgGuard<String> for StrGuard {
    async fn enter(&self) -> anyhow::Result<String> {
        Ok(self.value.clone())
    }
}

#[async_trait]
impl ArgGuard<String> for EnvGuard {
    async fn enter(&self) -> anyhow::Result<String> {
        Ok(std::env::var(self.name.as_str())?)
    }
}

#[async_trait]
impl ArgGuard<String> for InCloudFileGuard {
    async fn enter(&self) -> anyhow::Result<String> {
        self.param
            .download(self.bucket.clone(), self.temppath.to_path_buf())
            .await?;

        Ok(self.temppath.to_str().unwrap().to_string())
    }
}

#[async_trait]
impl ArgGuard<String> for OutCloudFileGuard {
    async fn enter(&self) -> anyhow::Result<String> {
        Ok(self.temppath.to_str().unwrap().to_string())
    }

    async fn exit(&self) -> anyhow::Result<()> {
        self.param
            .upload(self.bucket.clone(), self.temppath.to_path_buf())
            .await?;
        Ok(())
    }
}

#[async_trait]
impl ArgGuard<String> for FormatGuard {
    async fn enter(&self) -> anyhow::Result<String> {
        let args = guard_hashmap_args(&self.args, |param| self.ctx.push_guard(param)).await?;
        Ok(strfmt(self.tmpl.as_str(), &args)?)
    }

    async fn exit(&self) -> anyhow::Result<()> {
        self.ctx.pop_all().await.map(|_| ())
    }

    async fn clean(&mut self) {
        self.ctx.clean().await
    }
}

struct ContextStack {
    tempdir: TempDir,
    bucket: GridFSBucket,
    guards: ArcMtxRefCell<Vec<Box<dyn ArgGuard<String>>>>,
}

#[async_trait]
impl GuardStack<String> for ContextStack {
    async fn guard_param(&self, param: Param) -> Box<dyn ArgGuard<String>> {
        let bucket = self.bucket.clone();
        let new_temppath = || async {
            let temppath = tempfile::Builder::new()
                .tempfile_in(self.tempdir.path())
                .unwrap()
                .into_temp_path();
            temppath
        };

        match param {
            Param::StrParam { value } => Box::new(StrGuard { value }),
            Param::EnvParam { name } => Box::new(EnvGuard { name }),
            Param::FormatParam { tmpl, args } => Box::new(FormatGuard {
                tmpl,
                args,
                ctx: ContextStack {
                    bucket,
                    tempdir: TempDir::new_in(self.tempdir.path()).unwrap(),
                    guards: Arc::new(Mutex::new(RefCell::new(vec![]))),
                },
            }),
            param @ Param::InCloudFileParam { .. } => Box::new(InCloudFileGuard {
                temppath: new_temppath().await,
                bucket,
                param,
            }),
            param @ Param::OutCloudFileParam { .. } => Box::new(OutCloudFileGuard {
                temppath: new_temppath().await,
                bucket,
                param,
            }),
            param => unreachable!("Unaccepted Param {:#?} for server", param),
        }
    }

    fn guards(&self) -> &ArcMtxRefCell<Vec<Box<dyn ArgGuard<String>>>> {
        &self.guards
    }
}

pub(crate) struct MiddleImpl {
    ctx: ContextStack,
}

impl MiddleImpl {
    pub(crate) fn new(bucket: GridFSBucket, tempdir: TempDir) -> MiddleImpl {
        MiddleImpl {
            ctx: ContextStack {
                tempdir,
                bucket,
                guards: Arc::new(Mutex::new(RefCell::new(vec![]))),
            },
        }
    }
}

impl Drop for MiddleImpl {
    fn drop(&mut self) {
        futures::executor::block_on(async {
            self.ctx.clean().await;
        })
    }
}

#[async_trait]
impl Middle<RunRequest, RunResponse, RunRecipe, i32> for MiddleImpl {
    async fn transform_request(&self, run_request: RunRequest) -> anyhow::Result<RunRecipe> {
        guard_run_args(run_request, |param| self.ctx.push_guard(param)).await
    }

    async fn transform_response(
        &self,
        response: anyhow::Result<i32>,
    ) -> anyhow::Result<RunResponse> {
        self.ctx.pop_all().await?;
        Ok(RunResponse {
            return_code: response?,
            exc: None,
        })
    }
}
#[cfg(test)]
mod tests {
    use std::io::Write;
    use std::path::Path;

    use chain_ext::mongodb_gridfs::DatabaseExt;
    use fake::Fake;
    use tempfile::{tempdir, NamedTempFile};
    use test_utilities::docker;

    use super::*;

    //noinspection DuplicatedCode
    #[tokio::test]
    async fn test_invoke_middle() {
        let container = docker::Builder::new("mongo")
            .name("cmdproxy-test-server")
            .port_mapping(0, Some(27017))
            .build_disposable()
            .await;

        let bucket = mongodb::Client::with_uri_str(container.url.as_ref().unwrap())
            .await
            .unwrap()
            .database("cmdproxy-test-server-db")
            .bucket(None);

        let fake_workspace = tempdir().unwrap();

        let mut fake_input = NamedTempFile::new_in(fake_workspace.path()).unwrap();
        let fake_output = NamedTempFile::new_in(fake_workspace.path()).unwrap();

        let fake_stdout = NamedTempFile::new_in(fake_workspace.path()).unwrap();
        let fake_stderr = NamedTempFile::new_in(fake_workspace.path()).unwrap();

        let fake_input_content = (30..50).fake::<String>();
        let fake_stdout_content = (30..50).fake::<String>();

        fake_input.write_all(fake_input_content.as_bytes()).unwrap();

        let mut in_params = vec![];
        let mut out_params = vec![];

        let mut ipath = |path: &str, content: String| {
            let param = Param::ipath(path).as_cloud();
            in_params.push((param.clone(), content));
            param
        };

        let mut opath = |path: &str, content: String| {
            let param = Param::opath(path).as_cloud();
            out_params.push((param.clone(), content));
            param
        };

        let req = RunRequest::builder()
            .command(Param::str("/bin/bash"))
            .args(vec![
                Param::str("-c"),
                Param::format(
                    "echo '{content}' && cat {input} > {output}",
                    HashMap::from([
                        ("content", Param::str(fake_stdout_content.clone())),
                        (
                            "input",
                            ipath(
                                fake_input.path().to_str().unwrap(),
                                fake_input_content.clone(),
                            ),
                        ),
                        (
                            "output",
                            opath(
                                fake_output.path().to_str().unwrap(),
                                fake_input_content.clone(),
                            ),
                        ),
                    ]),
                ),
            ])
            .stdout(opath(
                fake_stdout.path().to_str().unwrap(),
                fake_stdout_content.clone(),
            ))
            .stderr(opath(fake_stderr.path().to_str().unwrap(), String::new()))
            .build();

        // mimic client upload input files
        for (param, content) in &in_params {
            param
                .upload_from_string(bucket.clone(), content)
                .await
                .unwrap();
        }

        {
            let server_tempdir = tempdir().unwrap();
            let invoke_middle = MiddleImpl::new(bucket.clone(), server_tempdir);
            let run_spec = invoke_middle.transform_request(req).await.unwrap();

            assert_eq!(run_spec.command, "/bin/bash");
            assert_eq!(run_spec.args.first().unwrap(), "-c");

            let pat =
                regex::Regex::new("echo '(?P<content>.*)' && cat (?P<input>.*) > (?P<output>.*)")
                    .unwrap();
            let cap = pat.captures(run_spec.args.last().unwrap()).unwrap();

            assert_eq!(cap.len(), 4);
            // assert string is formatted correctly
            assert!(cap.name("content").is_some());
            assert_eq!(
                cap.name("content").unwrap().as_str(),
                fake_stdout_content.as_str()
            );
            assert!(cap.name("input").is_some());
            assert!(cap.name("output").is_some());

            // assert all the inputs have been downloaded
            assert!(Path::new(cap.name("input").unwrap().as_str()).exists());
            assert_eq!(
                std::fs::read_to_string(cap.name("input").unwrap().as_str()).unwrap(),
                fake_input_content.as_str()
            );

            // mimic server to generate output files
            std::fs::write(
                cap.name("output").unwrap().as_str(),
                fake_input_content.as_str(),
            )
            .unwrap();
            std::fs::write(
                run_spec.stdout.unwrap().as_str(),
                fake_stdout_content.as_str(),
            )
            .unwrap();
            std::fs::write(run_spec.stderr.unwrap().as_str(), "").unwrap();

            let run_response = 0;
            invoke_middle
                .transform_response(Ok(run_response))
                .await
                .unwrap();
        }

        // assert all the outputs have been uploaded
        for (out_param, content) in out_params {
            assert!(out_param.exists_on_cloud(bucket.clone()).await.unwrap());
            assert_eq!(
                content,
                out_param.download_to_string(bucket.clone()).await.unwrap()
            );
        }
    }
}
