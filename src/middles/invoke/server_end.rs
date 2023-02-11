use std::cell::RefCell;
use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;

use anyhow::anyhow;
use celery::export::async_trait;
use chain_ext::path::file_ext::FileExt;
use log::debug;
use mongodb_gridfs::GridFSBucket;
use strfmt::strfmt;
use tempfile::{TempDir, TempPath};
use tokio::sync::Mutex;

use crate::middles::invoke::{
    guard_hashmap_args, guard_run_args, ArcMtxRefCell, ArgGuard, GuardStack, GuardStackData,
};
use crate::middles::Middle;
use crate::params::Param;
use crate::protocol::{RunRecipe, RunRequest, RunResponse};

struct Data {
    bucket: GridFSBucket,
    conf: Config,
    tempdir: TempDir,
    guards: Vec<Box<dyn ArgGuard<String, Data>>>,
    passed_env: HashMap<String, String>,
}

impl GuardStackData<String> for Data {
    fn pass_env(&mut self, key: String, val: &String) {
        self.passed_env.insert(key, val.clone());
    }

    fn guard_param(&self, param: Param) -> Box<dyn ArgGuard<String, Self>> {
        let new_temppath = |filepath: String| {
            let filename = Path::new(filepath.as_str())
                .file_name()
                .and_then(std::ffi::OsStr::to_str)
                .unwrap_or("");
            let temppath = tempfile::Builder::new()
                .suffix(filename)
                .tempfile_in(self.tempdir.path())
                .unwrap()
                .into_temp_path();
            temppath.remove().unwrap();
            temppath
        };

        match param {
            Param::StrParam { value } => Box::new(StrGuard { value }),
            Param::EnvParam { name } => Box::new(EnvGuard { name }),
            Param::CmdNameParam { name } => Box::new(CmdNameGuard { name }),
            Param::CmdPathParam { path } => Box::new(CmdPathGuard { path }),
            Param::FormatParam { tmpl, args } => Box::new(FormatGuard { tmpl, args }),
            param @ Param::InCloudFileParam { .. } => Box::new(InCloudFileGuard {
                temppath: new_temppath(param.filepath().to_string()),
                param,
            }),
            param @ Param::OutCloudFileParam { .. } => Box::new(OutCloudFileGuard {
                temppath: new_temppath(param.filepath().to_string()),
                param,
            }),
            param => unreachable!("Unaccepted Param {:#?} for server", param),
        }
    }

    fn guards(&self) -> &Vec<Box<dyn ArgGuard<String, Self>>> {
        &self.guards
    }

    fn guards_mut(&mut self) -> &mut Vec<Box<dyn ArgGuard<String, Self>>> {
        &mut self.guards
    }
}

struct StrGuard {
    value: String,
}

struct EnvGuard {
    name: String,
}

struct CmdNameGuard {
    name: String,
}

struct CmdPathGuard {
    path: String,
}

struct InCloudFileGuard {
    temppath: TempPath,
    param: Param,
}

struct OutCloudFileGuard {
    temppath: TempPath,
    param: Param,
}

struct FormatGuard {
    tmpl: String,
    args: HashMap<String, Param>,
}

#[async_trait]
impl ArgGuard<String, Data> for StrGuard {
    async fn enter(&self, _: &ArcMtxRefCell<Data>) -> anyhow::Result<String> {
        Ok(self.value.clone())
    }
}

#[async_trait]
impl ArgGuard<String, Data> for EnvGuard {
    async fn enter(&self, data: &ArcMtxRefCell<Data>) -> anyhow::Result<String> {
        let data = data.lock().await;
        let data = data.borrow();
        Ok(std::env::var(self.name.as_str()).unwrap_or_else(|_| {
            data.passed_env
                .get(self.name.as_str())
                .map(Clone::clone)
                .unwrap_or_else(String::new)
        }))
    }
}

#[async_trait]
impl ArgGuard<String, Data> for CmdNameGuard {
    async fn enter(&self, data: &ArcMtxRefCell<Data>) -> anyhow::Result<String> {
        let data = data.lock().await;
        let data = data.borrow_mut();
        let command_palette = &data.conf.command_palette;
        if let Some(command) = command_palette.get(self.name.as_str()) {
            Ok(command.clone())
        } else {
            Err(anyhow!(
                "Command `{}' not found in command-palette:{:#?}\n",
                self.name,
                command_palette
            ))
        }
    }
}

#[async_trait]
impl ArgGuard<String, Data> for CmdPathGuard {
    async fn enter(&self, _: &ArcMtxRefCell<Data>) -> anyhow::Result<String> {
        Ok(self.path.clone())
    }
}

#[async_trait]
impl ArgGuard<String, Data> for InCloudFileGuard {
    async fn enter(&self, data: &ArcMtxRefCell<Data>) -> anyhow::Result<String> {
        debug!(
            "Download cloud input {} to {}...",
            self.param.cloud_url(),
            self.temppath.to_str().unwrap(),
        );

        let bucket = {
            let data = data.lock().await;
            let data = data.borrow();
            data.bucket.clone()
        };
        self.param
            .download(bucket, self.temppath.to_path_buf())
            .await?;

        Ok(self.temppath.to_str().unwrap().to_string())
    }
}

#[async_trait]
impl ArgGuard<String, Data> for OutCloudFileGuard {
    async fn enter(&self, _: &ArcMtxRefCell<Data>) -> anyhow::Result<String> {
        Ok(self.temppath.to_str().unwrap().to_string())
    }

    async fn exit(&self, data: &ArcMtxRefCell<Data>) -> anyhow::Result<()> {
        if self.temppath.exists() {
            let bucket = {
                let data = data.lock().await;
                let data = data.borrow();
                data.bucket.clone()
            };

            self.param
                .upload(bucket, self.temppath.to_path_buf())
                .await?;
        }
        debug!(
            "Upload local output {} to {}...",
            self.temppath.to_str().unwrap(),
            self.param.cloud_url(),
        );
        Ok(())
    }
}

#[async_trait]
impl ArgGuard<String, Data> for FormatGuard {
    async fn enter(&self, data: &ArcMtxRefCell<Data>) -> anyhow::Result<String> {
        let args = guard_hashmap_args(&self.args, |param| {
            GuardStackData::push_guard(data, param, None)
        })
        .await?;
        Ok(strfmt(self.tmpl.as_str(), &args)?)
    }
}

struct ContextStack {
    data: ArcMtxRefCell<Data>,
}

#[async_trait]
impl GuardStack<String, Data> for ContextStack {
    fn data(&self) -> &ArcMtxRefCell<Data> {
        &self.data
    }
}

#[derive(Clone)]
pub(crate) struct Config {
    pub(crate) command_palette: HashMap<String, String>,
}

pub(crate) struct MiddleImpl {
    ctx: ContextStack,
}

impl MiddleImpl {
    pub(crate) fn new(bucket: GridFSBucket, tempdir: TempDir, conf: Config) -> MiddleImpl {
        MiddleImpl {
            ctx: ContextStack {
                data: Arc::new(Mutex::new(RefCell::new(Data {
                    bucket,
                    conf,
                    tempdir,
                    guards: Vec::new(),
                    passed_env: HashMap::new(),
                }))),
            },
        }
    }
}

#[async_trait]
impl Middle<RunRequest, RunResponse, RunRecipe, i32> for MiddleImpl {
    async fn transform_request(&self, run_request: RunRequest) -> anyhow::Result<RunRecipe> {
        guard_run_args(run_request, |param, key| self.ctx.push_guard(param, key)).await
    }

    async fn transform_response(
        &self,
        response: anyhow::Result<i32>,
    ) -> anyhow::Result<RunResponse> {
        self.ctx.pop_all_guards().await?;
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
            .bind_port_as_default(Some("0"), "27017")
            .build_disposable()
            .await;

        let bucket = mongodb::Client::with_uri_str(container.url())
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
        let conf = Config {
            command_palette: HashMap::<String, String>::new(),
        };

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

        // mimic client upload input files
        for (param, content) in &in_params {
            param
                .upload_from_string(bucket.clone(), content)
                .await
                .unwrap();
        }

        {
            let server_tempdir = tempdir().unwrap();
            let invoke_middle = MiddleImpl::new(bucket.clone(), server_tempdir, conf);
            let run_spec = invoke_middle.transform_request(req).await.unwrap();

            assert_eq!(run_spec.command, "/bin/sh");
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
