use std::cell::RefCell;
use std::collections::HashMap;
use std::sync::Arc;

use celery::export::async_trait;
use log::debug;
use mongodb_gridfs::GridFSBucket;
use tokio::sync::Mutex;

use crate::middles::invoke::{
    guard_hashmap_args, push_guard, ArcMtxRefCell, ArgGuard, GuardStack, GuardStackData,
    InvokeMiddle,
};
use crate::params::Param;

struct Data {
    bucket: GridFSBucket,
    guards: Vec<Box<dyn ArgGuard<Param, Data>>>,
}

impl GuardStackData<Param, Param> for Data {
    fn pass_env(&mut self, _: String, _: &Param) {}

    fn guard_param(&self, param: Param) -> Box<dyn ArgGuard<Param, Self>> {
        match param {
            Param::StrParam { value } => Box::new(StrGuard { value }),
            Param::EnvParam { name } => Box::new(EnvGuard { name }),
            Param::RemoteEnvParam { name } => Box::new(RemoteEnvGuard { name }),
            Param::CmdNameParam { name } => Box::new(CmdNameGuard { name }),
            Param::CmdPathParam { path } => Box::new(CmdPathGuard { path }),
            Param::FormatParam { tmpl, args } => Box::new(FormatGuard { tmpl, args }),
            param @ Param::InLocalFileParam { .. } => Box::new(InLocalFileGuard { param }),
            param @ Param::OutLocalFileParam { .. } => Box::new(OutLocalFileGuard { param }),
            param @ Param::InCloudFileParam { .. } => Box::new(InCloudFileGuard { param }),
            param @ Param::OutCloudFileParam { .. } => Box::new(OutCloudFileGuard { param }),
        }
    }

    fn guards(&self) -> &Vec<Box<dyn ArgGuard<Param, Self>>> {
        &self.guards
    }

    fn guards_mut(&mut self) -> &mut Vec<Box<dyn ArgGuard<Param, Self>>> {
        &mut self.guards
    }
}

struct StrGuard {
    value: String,
}

struct EnvGuard {
    name: String,
}

struct RemoteEnvGuard {
    name: String,
}

struct CmdNameGuard {
    name: String,
}

struct CmdPathGuard {
    path: String,
}

struct InCloudFileGuard {
    param: Param,
}

struct OutCloudFileGuard {
    param: Param,
}

struct InLocalFileGuard {
    param: Param,
}

struct OutLocalFileGuard {
    param: Param,
}

struct FormatGuard {
    tmpl: String,
    args: HashMap<String, Param>,
}

#[async_trait]
impl ArgGuard<Param, Data> for StrGuard {
    async fn enter(&self, _: &ArcMtxRefCell<Data>) -> anyhow::Result<Param> {
        Ok(Param::StrParam {
            value: self.value.clone(),
        })
    }
}

#[async_trait]
impl ArgGuard<Param, Data> for EnvGuard {
    async fn enter(&self, _: &ArcMtxRefCell<Data>) -> anyhow::Result<Param> {
        Ok(Param::StrParam {
            value: std::env::var(self.name.as_str())?,
        })
    }
}

#[async_trait]
impl ArgGuard<Param, Data> for RemoteEnvGuard {
    async fn enter(&self, _: &ArcMtxRefCell<Data>) -> anyhow::Result<Param> {
        Ok(Param::EnvParam {
            name: self.name.clone(),
        })
    }
}

#[async_trait]
impl ArgGuard<Param, Data> for CmdNameGuard {
    async fn enter(&self, _: &ArcMtxRefCell<Data>) -> anyhow::Result<Param> {
        Ok(Param::CmdNameParam {
            name: self.name.clone(),
        })
    }
}

#[async_trait]
impl ArgGuard<Param, Data> for CmdPathGuard {
    async fn enter(&self, _: &ArcMtxRefCell<Data>) -> anyhow::Result<Param> {
        Ok(Param::CmdPathParam {
            path: self.path.clone(),
        })
    }
}

#[async_trait]
impl ArgGuard<Param, Data> for InCloudFileGuard {
    async fn enter(&self, _: &ArcMtxRefCell<Data>) -> anyhow::Result<Param> {
        Ok(self.param.clone())
    }
}

#[async_trait]
impl ArgGuard<Param, Data> for InLocalFileGuard {
    async fn enter(&self, data: &ArcMtxRefCell<Data>) -> anyhow::Result<Param> {
        debug!(
            "Upload local input {} to {}...",
            self.param.filepath(),
            self.param.cloud_url(),
        );

        let bucket = {
            let data = data.lock().await;
            let data = data.borrow();
            data.bucket.clone()
        };
        self.param.upload_inplace(bucket).await?;
        Ok(self.param.as_cloud())
    }

    //noinspection DuplicatedCode
    async fn exit(&self, data: &ArcMtxRefCell<Data>) -> anyhow::Result<()> {
        let bucket = {
            let data = data.lock().await;
            let data = data.borrow();
            data.bucket.clone()
        };
        self.param
            .remove_from_cloud(bucket)
            .await
            .map_err(Into::into)
    }
}

#[async_trait]
impl ArgGuard<Param, Data> for OutCloudFileGuard {
    async fn enter(&self, _: &ArcMtxRefCell<Data>) -> anyhow::Result<Param> {
        Ok(self.param.as_cloud())
    }
}

#[async_trait]
impl ArgGuard<Param, Data> for OutLocalFileGuard {
    async fn enter(&self, _: &ArcMtxRefCell<Data>) -> anyhow::Result<Param> {
        Ok(self.param.as_cloud())
    }

    async fn exit(&self, data: &ArcMtxRefCell<Data>) -> anyhow::Result<()> {
        debug!(
            "Download cloud output {} to {}...",
            self.param.cloud_url(),
            self.param.filepath()
        );

        let bucket = {
            let data = data.lock().await;
            let data = data.borrow();
            data.bucket.clone()
        };
        self.param.download_inplace(bucket.clone()).await?;
        self.param
            .remove_from_cloud(bucket)
            .await
            .unwrap_or_default();
        Ok(())
    }
}

#[async_trait]
impl ArgGuard<Param, Data> for FormatGuard {
    async fn enter(&self, data: &ArcMtxRefCell<Data>) -> anyhow::Result<Param> {
        let args = guard_hashmap_args(&self.args, |param| push_guard(data, param, None)).await?;
        Ok(Param::FormatParam {
            tmpl: self.tmpl.clone(),
            args,
        })
    }
}

struct ContextStack {
    data: ArcMtxRefCell<Data>,
}

#[async_trait]
impl GuardStack<Param, Param, Data> for ContextStack {
    fn data(&self) -> &ArcMtxRefCell<Data> {
        &self.data
    }
}

pub(crate) struct MiddleImpl {
    ctx: ContextStack,
}

impl MiddleImpl {
    //noinspection DuplicatedCode
    pub fn new(bucket: GridFSBucket) -> MiddleImpl {
        MiddleImpl {
            ctx: ContextStack {
                data: Arc::new(Mutex::new(RefCell::new(Data {
                    bucket,
                    guards: Vec::new(),
                }))),
            },
        }
    }
}

#[async_trait]
impl InvokeMiddle<Param, Param> for MiddleImpl {
    async fn push_guard(&self, param: Param, key: Option<String>) -> anyhow::Result<Param> {
        self.ctx.push_guard(param, key).await
    }

    async fn pop_all_guards(&self) -> anyhow::Result<Vec<()>> {
        self.ctx.pop_all_guards().await
    }
}

#[cfg(test)]
mod tests {
    use std::io::Write;

    use chain_ext::mongodb_gridfs::DatabaseExt;
    use fake::Fake;
    use tempfile::{tempdir, NamedTempFile};
    use test_utilities::docker;

    use crate::middles::Middle;
    use crate::protocol::{RunRequest, RunResponse};

    use super::*;

    //noinspection DuplicatedCode
    #[tokio::test]
    async fn test_invoke_middle() {
        let container = docker::Builder::new("mongo")
            .name("cmdproxy-test-client")
            .bind_port_as_default(Some("0"), "27017")
            .build_disposable()
            .await;

        let bucket = mongodb::Client::with_uri_str(container.url())
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
            .command(Param::str("/bin/sh"))
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
                    Param::StrParam { value } if value == "/bin/sh"));
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
