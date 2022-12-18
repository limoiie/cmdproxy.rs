use std::cell::RefCell;
use std::collections::HashMap;
use std::sync::Arc;

use celery::export::async_trait;
use mongodb_gridfs::GridFSBucket;
use tokio::sync::Mutex;

use crate::middles::invoke::{
    guard_hashmap_args, guard_run_args, ArcMtxRefCell, ArgGuard, GuardStack,
};
use crate::middles::Middle;
use crate::params::Param;
use crate::protocol::{RunRequest, RunResponse};

struct StrGuard {
    value: String,
}

struct EnvGuard {
    name: String,
}

struct RemoteEnvGuard {
    name: String,
}

struct InCloudFileGuard {
    param: Param,
}

struct OutCloudFileGuard {
    param: Param,
}

struct InLocalFileGuard {
    bucket: GridFSBucket,
    param: Param,
}

struct OutLocalFileGuard {
    bucket: GridFSBucket,
    param: Param,
}

struct FormatGuard {
    tmpl: String,
    args: HashMap<String, Param>,
    ctx: ContextStack,
}

#[async_trait]
impl ArgGuard<Param> for StrGuard {
    async fn enter(&self) -> anyhow::Result<Param> {
        Ok(Param::StrParam {
            value: self.value.clone(),
        })
    }
}

#[async_trait]
impl ArgGuard<Param> for EnvGuard {
    async fn enter(&self) -> anyhow::Result<Param> {
        Ok(Param::StrParam {
            value: std::env::var(self.name.as_str())?,
        })
    }
}

#[async_trait]
impl ArgGuard<Param> for RemoteEnvGuard {
    async fn enter(&self) -> anyhow::Result<Param> {
        Ok(Param::EnvParam {
            name: self.name.clone(),
        })
    }
}

#[async_trait]
impl ArgGuard<Param> for InCloudFileGuard {
    async fn enter(&self) -> anyhow::Result<Param> {
        Ok(self.param.clone())
    }
}

#[async_trait]
impl ArgGuard<Param> for InLocalFileGuard {
    async fn enter(&self) -> anyhow::Result<Param> {
        self.param.upload_inplace(self.bucket.clone()).await?;
        Ok(self.param.as_cloud())
    }

    async fn clean(&mut self) {
        self.param
            .remove_from_cloud(self.bucket.clone())
            .await
            .unwrap_or_default();
    }
}

#[async_trait]
impl ArgGuard<Param> for OutCloudFileGuard {
    async fn enter(&self) -> anyhow::Result<Param> {
        Ok(self.param.as_cloud())
    }
}

#[async_trait]
impl ArgGuard<Param> for OutLocalFileGuard {
    async fn enter(&self) -> anyhow::Result<Param> {
        Ok(self.param.as_cloud())
    }

    async fn exit(&self) -> anyhow::Result<()> {
        self.param.download_inplace(self.bucket.clone()).await?;
        Ok(())
    }

    async fn clean(&mut self) {
        self.param
            .remove_from_cloud(self.bucket.clone())
            .await
            .unwrap_or_default();
    }
}

#[async_trait]
impl ArgGuard<Param> for FormatGuard {
    async fn enter(&self) -> anyhow::Result<Param> {
        let args = guard_hashmap_args(&self.args, |param| self.ctx.push_guard(param)).await?;
        Ok(Param::FormatParam {
            tmpl: self.tmpl.clone(),
            args,
        })
    }

    async fn exit(&self) -> anyhow::Result<()> {
        self.ctx.pop_all().await.map(|_| ())
    }

    async fn clean(&mut self) {
        self.ctx.clean().await
    }
}

struct ContextStack {
    bucket: GridFSBucket,
    guards: ArcMtxRefCell<Vec<Box<dyn ArgGuard<Param>>>>,
}

#[async_trait]
impl GuardStack<Param> for ContextStack {
    async fn guard_param(&self, param: Param) -> Box<dyn ArgGuard<Param>> {
        let bucket = self.bucket.clone();
        match param {
            Param::StrParam { value } => Box::new(StrGuard { value }),
            Param::EnvParam { name } => Box::new(EnvGuard { name }),
            Param::RemoteEnvParam { name } => Box::new(RemoteEnvGuard { name }),
            Param::FormatParam { tmpl, args } => Box::new(FormatGuard {
                tmpl,
                args,
                ctx: ContextStack {
                    bucket: self.bucket.clone(),
                    guards: Arc::new(Mutex::new(RefCell::new(vec![]))),
                },
            }),
            param @ Param::InLocalFileParam { .. } => Box::new(InLocalFileGuard { bucket, param }),
            param @ Param::OutLocalFileParam { .. } => {
                Box::new(OutLocalFileGuard { bucket, param })
            }
            param @ Param::InCloudFileParam { .. } => Box::new(InCloudFileGuard { param }),
            param @ Param::OutCloudFileParam { .. } => Box::new(OutCloudFileGuard { param }),
        }
    }

    fn guards(&self) -> &Arc<Mutex<RefCell<Vec<Box<dyn ArgGuard<Param>>>>>> {
        &self.guards
    }
}

pub(crate) struct MiddleImpl {
    ctx: ContextStack,
}

impl MiddleImpl {
    pub fn new(bucket: GridFSBucket) -> MiddleImpl {
        MiddleImpl {
            ctx: ContextStack {
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
impl Middle<RunRequest, RunResponse, RunRequest, RunResponse> for MiddleImpl {
    async fn transform_request(&self, run_request: RunRequest) -> anyhow::Result<RunRequest> {
        guard_run_args(run_request, |param| self.ctx.push_guard(param)).await
    }

    async fn transform_response(
        &self,
        response: anyhow::Result<RunResponse>,
    ) -> anyhow::Result<RunResponse> {
        self.ctx.pop_all().await?;
        response
    }
}

#[cfg(test)]
mod tests {
    use std::io::Write;

    use chain_ext::mongodb_gridfs::DatabaseExt;
    use fake::Fake;
    use tempfile::{tempdir, NamedTempFile};
    use test_utilities::docker;

    use crate::protocol::RunResponse;

    use super::*;

    //noinspection DuplicatedCode
    #[tokio::test]
    async fn test_invoke_middle() {
        let container = docker::Builder::new("mongo")
            .name("cmdproxy-test-client")
            .port_mapping(0, Some(27017))
            .build_disposable()
            .await;

        let bucket = mongodb::Client::with_uri_str(container.url.as_ref().unwrap())
            .await
            .unwrap()
            .database("cmdproxy-test-client-db")
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
            let param = Param::ipath(path);
            in_params.push((param.clone(), content));
            param
        };

        let mut opath = |path: &str, content: String| {
            let param = Param::opath(path);
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

        {
            let invoke_middle = MiddleImpl::new(bucket.clone());
            let wrapped_req = invoke_middle.transform_request(req).await.unwrap();

            assert!(matches!(wrapped_req.command,
                    Param::StrParam { value } if value == "/bin/bash"));
            assert!(matches!(
                wrapped_req.args.first().unwrap(),
                Param::StrParam { value } if value == "-c"
            ));

            let (in_param, out_param) = match wrapped_req.args.last().unwrap() {
                Param::FormatParam { args, .. } => {
                    // assert file params have been transformed into cloud ones
                    (args["input"].clone(), args["output"].clone())
                }
                _ => panic!(),
            };
            let stdout_param = wrapped_req.stdout.unwrap();
            let stderr_param = wrapped_req.stderr.unwrap();

            let wrapped_in_params = vec![in_param];
            let wrapped_out_params = vec![out_param, stdout_param, stderr_param];

            // assert file params have been transformed as cloud param
            for (wrapped_in_param, (_, content)) in wrapped_in_params.iter().zip(in_params.iter()) {
                assert!(wrapped_in_param.is_input() && wrapped_in_param.is_cloud());

                // assert input files have been uploaded
                let uploaded_content = wrapped_in_param
                    .download_to_string(bucket.clone())
                    .await
                    .unwrap();
                assert_eq!(content, &uploaded_content);
            }

            // assert file params have been transformed as cloud param
            for wrapped_out_param in wrapped_out_params {
                assert!(wrapped_out_param.is_output() && wrapped_out_param.is_cloud());
            }

            // mimic server to upload output files after running
            for (out_param, content) in &out_params {
                out_param
                    .upload_from_string(bucket.clone(), content.as_str())
                    .await
                    .unwrap();
            }

            let run_response = RunResponse {
                return_code: 0,
                exc: None,
            };
            invoke_middle
                .transform_response(Ok(run_response))
                .await
                .unwrap();
        }

        // assert all the inputs have been removed from the cloud
        for (in_param, _content) in in_params {
            assert!(!in_param.exists_on_cloud(bucket.clone()).await.unwrap());
        }

        // assert all the outputs have been downloaded, and been removed from the cloud
        for (out_param, content) in out_params {
            assert!(!out_param.exists_on_cloud(bucket.clone()).await.unwrap());
            assert_eq!(
                content,
                std::fs::read_to_string(out_param.filepath()).unwrap()
            );
        }
    }
}
