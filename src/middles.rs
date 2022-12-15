use std::cell::RefCell;
use std::collections::HashMap;
use std::iter;
use std::sync::Arc;

use celery::export::async_trait;
use mongodb_gridfs::GridFSBucket;
use strfmt::strfmt;
use tempfile::TempDir;
use tokio::sync::Mutex;

use crate::params::Param;
use crate::protocol::RunRequest;

#[doc(hidden)]
#[macro_export]
macro_rules! __apply_middles_in_stack {
    (
        $arg:expr,
        $func:expr,
        [ $middle:expr $(, $middles:expr )* ]
    ) => {{
        let middle = $middle;
        let arg = middle.transform_request($arg).await;
        let arg = $crate::__apply_middles_in_stack!(arg, $func, [ $($middles),* ]);
        let arg = middle.transform_response(arg).await;
        arg
    }};
    ( $arg:expr, $func:expr, [] ) => { $func($arg).await };
}

#[macro_export]
macro_rules! apply_middles {
    (
        $init:expr,
        $( @$middles:expr )+
        => $func:ident
    ) => {
        $crate::__apply_middles_in_stack!( $init, $func, [ $( $middles ),* ] )
    };
}

#[async_trait]
pub(crate) trait Middle<Request, Response, IRequest, IResponse>
where
    Request: Send,
    Response: Send,
    IRequest: Send,
    IResponse: Send,
{
    async fn transform_request(&self, request: Request) -> IRequest;

    async fn transform_response(&self, response: IResponse) -> Response;
}

pub(crate) mod client {
    use crate::protocol::RunResponse;

    use super::*;

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
        ctx: Arc<Mutex<ContextStack>>,
    }

    #[async_trait]
    trait ArgGuard: Send + Sync {
        async fn enter(&self) -> Param;
        async fn exit(&mut self) {}
    }

    #[async_trait]
    impl ArgGuard for StrGuard {
        async fn enter(&self) -> Param {
            Param::StrParam {
                value: self.value.clone(),
            }
        }
    }

    #[async_trait]
    impl ArgGuard for EnvGuard {
        async fn enter(&self) -> Param {
            Param::StrParam {
                value: std::env::var(self.name.as_str()).unwrap(),
            }
        }
    }

    #[async_trait]
    impl ArgGuard for RemoteEnvGuard {
        async fn enter(&self) -> Param {
            Param::EnvParam {
                name: self.name.clone(),
            }
        }
    }

    #[async_trait]
    impl ArgGuard for InCloudFileGuard {
        async fn enter(&self) -> Param {
            self.param.clone()
        }
    }

    #[async_trait]
    impl ArgGuard for InLocalFileGuard {
        async fn enter(&self) -> Param {
            self.param
                .upload_inplace(self.bucket.clone())
                .await
                .unwrap();
            self.param.as_cloud()
        }

        async fn exit(&mut self) {
            self.param
                .remove_from_cloud(self.bucket.clone())
                .await
                .unwrap();
        }
    }

    #[async_trait]
    impl ArgGuard for OutCloudFileGuard {
        async fn enter(&self) -> Param {
            self.param.as_cloud()
        }
    }

    #[async_trait]
    impl ArgGuard for OutLocalFileGuard {
        async fn enter(&self) -> Param {
            self.param.as_cloud()
        }

        async fn exit(&mut self) {
            self.param
                .download_inplace(self.bucket.clone())
                .await
                .unwrap();
            self.param
                .remove_from_cloud(self.bucket.clone())
                .await
                .unwrap();
        }
    }

    #[async_trait]
    impl ArgGuard for FormatGuard {
        //noinspection DuplicatedCode
        async fn enter(&self) -> Param {
            let args = futures::future::join_all(
                self.args
                    .values()
                    .map(|param| ContextStack::wrap_arg(self.ctx.clone(), param.clone())),
            )
            .await
            .into_iter()
            .zip(self.args.keys())
            .map(|(param, name)| (name.clone(), param))
            .collect();

            Param::FormatParam {
                tmpl: self.tmpl.clone(),
                args,
            }
        }
    }

    struct ContextStack {
        bucket: GridFSBucket,
        guards: RefCell<Vec<Box<dyn ArgGuard>>>,
    }

    impl ContextStack {
        pub async fn wrap_arg(ctx: Arc<Mutex<ContextStack>>, arg: Param) -> Param {
            let bucket = ctx.lock().await.bucket.clone();

            let guard: Box<dyn ArgGuard> = match arg {
                Param::StrParam { value } => Box::new(StrGuard { value }),
                Param::EnvParam { name } => Box::new(EnvGuard { name }),
                Param::RemoteEnvParam { name } => Box::new(RemoteEnvGuard { name }),
                Param::FormatParam { tmpl, args } => Box::new(FormatGuard {
                    tmpl,
                    args,
                    ctx: ctx.clone(),
                }),
                param @ Param::InLocalFileParam { .. } => {
                    Box::new(InLocalFileGuard { bucket, param })
                }
                param @ Param::OutLocalFileParam { .. } => {
                    Box::new(OutLocalFileGuard { bucket, param })
                }
                param @ Param::InCloudFileParam { .. } => Box::new(InCloudFileGuard { param }),
                param @ Param::OutCloudFileParam { .. } => Box::new(OutCloudFileGuard { param }),
            };

            let param = guard.enter().await;

            let mutex = ctx.lock().await;
            mutex.guards.borrow_mut().push(guard);

            param
        }

        //noinspection DuplicatedCode
        async fn drop_guards(&mut self) {
            let mut guards = self.guards.take();
            futures::future::join_all(guards.iter_mut().map(|guard| guard.exit())).await;
        }
    }

    pub(crate) struct ProxyInvokeMiddle {
        ctx: Arc<Mutex<ContextStack>>,
    }

    impl ProxyInvokeMiddle {
        pub fn new(bucket: GridFSBucket) -> ProxyInvokeMiddle {
            ProxyInvokeMiddle {
                ctx: Arc::new(Mutex::new(ContextStack {
                    bucket,
                    guards: RefCell::new(vec![]),
                })),
            }
        }
    }

    //noinspection DuplicatedCode
    impl Drop for ProxyInvokeMiddle {
        fn drop(&mut self) {
            futures::executor::block_on(async {
                let mut ctx = self.ctx.lock().await;
                ctx.drop_guards().await;
            })
        }
    }

    #[async_trait]
    impl Middle<RunRequest, RunResponse, RunRequest, RunResponse> for ProxyInvokeMiddle {
        //noinspection DuplicatedCode
        async fn transform_request(&self, run_request: RunRequest) -> RunRequest {
            let has_stdout = run_request.stdout.as_ref().map(|_| ());
            let has_stderr = run_request.stderr.as_ref().map(|_| ());

            let env_keys = run_request
                .env
                .as_ref()
                .map(|m| m.keys().map(Clone::clone).collect::<Vec<_>>());

            let n_to_downloads = run_request.to_downloads.as_ref().map(Vec::len);
            let n_to_uploads = run_request.to_uploads.as_ref().map(Vec::len);

            let mut wrapped_args = futures::future::join_all(
                run_request
                    .args
                    .into_iter()
                    .chain(iter::once(run_request.command))
                    .chain(run_request.stdout.into_iter())
                    .chain(run_request.stderr.into_iter())
                    .chain(run_request.env.into_iter().flat_map(|m| m.into_values()))
                    .chain(
                        run_request
                            .to_downloads
                            .into_iter()
                            .flat_map(|v| v.into_iter()),
                    )
                    .chain(
                        run_request
                            .to_uploads
                            .into_iter()
                            .flat_map(|v| v.into_iter()),
                    )
                    .map(|param| ContextStack::wrap_arg(self.ctx.clone(), param)),
            )
            .await;

            let cwd = run_request.cwd;

            let to_downloads = n_to_downloads.map(|n_to_downloads| {
                (0..n_to_downloads)
                    .rev()
                    .map(|_| wrapped_args.pop().unwrap())
                    .collect()
            });
            let to_uploads = n_to_uploads.map(|n_to_uploads| {
                (0..n_to_uploads)
                    .rev()
                    .map(|_| wrapped_args.pop().unwrap())
                    .collect()
            });

            let env = env_keys.map(|keys| {
                keys.into_iter()
                    .rev()
                    .map(|key| (key, wrapped_args.pop().unwrap()))
                    .collect()
            });
            let stderr = has_stderr.and_then(|_| wrapped_args.pop());
            let stdout = has_stdout.and_then(|_| wrapped_args.pop());
            let command = wrapped_args.pop().unwrap();
            let args = wrapped_args;

            RunRequest {
                command,
                args,
                cwd,
                env,
                to_downloads,
                to_uploads,
                stdout,
                stderr,
            }
        }

        async fn transform_response(&self, response: RunResponse) -> RunResponse {
            response
        }
    }

    pub(crate) struct PackAndSerializeMiddle {}

    impl PackAndSerializeMiddle {
        pub(crate) fn new() -> PackAndSerializeMiddle {
            PackAndSerializeMiddle {}
        }
    }

    #[async_trait]
    impl Middle<RunRequest, RunResponse, String, String> for PackAndSerializeMiddle {
        async fn transform_request(&self, request: RunRequest) -> String {
            serde_json::to_string(&request).unwrap()
        }

        async fn transform_response(&self, response: String) -> RunResponse {
            serde_json::from_str(response.as_str()).unwrap()
        }
    }
}

pub(crate) mod server {
    use tempfile::TempPath;

    use crate::protocol::RunResponse;

    use super::*;

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
        ctx: Arc<Mutex<ContextStack>>,
    }

    #[async_trait]
    trait ArgGuard: Send + Sync {
        async fn enter(&self) -> String;
        async fn exit(&mut self) {}
    }

    #[async_trait]
    impl ArgGuard for StrGuard {
        async fn enter(&self) -> String {
            self.value.clone()
        }
    }

    #[async_trait]
    impl ArgGuard for EnvGuard {
        async fn enter(&self) -> String {
            std::env::var(self.name.as_str()).unwrap()
        }
    }

    #[async_trait]
    impl ArgGuard for InCloudFileGuard {
        async fn enter(&self) -> String {
            self.param
                .download(self.bucket.clone(), self.temppath.to_path_buf())
                .await
                .unwrap();

            self.temppath.to_str().unwrap().to_string()
        }
    }

    #[async_trait]
    impl ArgGuard for OutCloudFileGuard {
        async fn enter(&self) -> String {
            self.temppath.to_str().unwrap().to_string()
        }

        async fn exit(&mut self) {
            self.param
                .upload(self.bucket.clone(), self.temppath.to_path_buf())
                .await
                .unwrap();
        }
    }

    #[async_trait]
    impl ArgGuard for FormatGuard {
        //noinspection DuplicatedCode
        async fn enter(&self) -> String {
            let args = futures::future::join_all(
                self.args
                    .values()
                    .map(|param| ContextStack::wrap_arg(self.ctx.clone(), param.clone())),
            )
            .await
            .into_iter()
            .zip(self.args.keys())
            .map(|(param, name)| (name.clone(), param))
            .collect();

            strfmt(self.tmpl.as_str(), &args).unwrap()
        }
    }

    struct ContextStack {
        tempdir: TempDir,
        bucket: GridFSBucket,
        guards: RefCell<Vec<Box<dyn ArgGuard>>>,
    }

    impl ContextStack {
        pub async fn wrap_arg(ctx: Arc<Mutex<ContextStack>>, arg: Param) -> String {
            let bucket = ctx.lock().await.bucket.clone();
            let new_temppath = || async {
                let tempdir = &ctx.lock().await.tempdir;
                let temppath = tempfile::Builder::new()
                    .tempfile_in(tempdir.path())
                    .unwrap()
                    .into_temp_path();
                temppath
            };

            let guard: Box<dyn ArgGuard> = match arg {
                Param::StrParam { value } => Box::new(StrGuard { value }),
                Param::EnvParam { name } => Box::new(EnvGuard { name }),
                Param::FormatParam { tmpl, args } => Box::new(FormatGuard {
                    tmpl,
                    args,
                    ctx: ctx.clone(),
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
            };

            let param = guard.enter().await;

            let mutex = ctx.lock().await;
            mutex.guards.borrow_mut().push(guard);

            param
        }

        //noinspection DuplicatedCode
        async fn drop_guards(&mut self) {
            let mut guards = self.guards.take();
            futures::future::join_all(guards.iter_mut().map(|guard| guard.exit())).await;
        }
    }

    #[derive(Debug)]
    pub(crate) struct RunSpec {
        pub(crate) command: String,
        pub(crate) args: Vec<String>,
        pub(crate) cwd: Option<String>,
        pub(crate) env: Option<HashMap<String, String>>,
        pub(crate) stdout: Option<String>,
        pub(crate) stderr: Option<String>,
    }

    pub(crate) struct ProxyInvokeMiddle {
        ctx: Arc<Mutex<ContextStack>>,
    }

    //noinspection DuplicatedCode
    impl ProxyInvokeMiddle {
        pub(crate) fn new(bucket: GridFSBucket, tempdir: TempDir) -> ProxyInvokeMiddle {
            ProxyInvokeMiddle {
                ctx: Arc::new(Mutex::new(ContextStack {
                    tempdir,
                    bucket,
                    guards: RefCell::new(vec![]),
                })),
            }
        }
    }

    //noinspection DuplicatedCode
    impl Drop for ProxyInvokeMiddle {
        fn drop(&mut self) {
            futures::executor::block_on(async {
                let mut ctx = self.ctx.lock().await;
                ctx.drop_guards().await;
            })
        }
    }

    #[async_trait]
    impl Middle<RunRequest, RunResponse, RunSpec, i32> for ProxyInvokeMiddle {
        //noinspection DuplicatedCode
        async fn transform_request(&self, run_request: RunRequest) -> RunSpec {
            let has_stdout = run_request.stdout.as_ref().map(|_| ());
            let has_stderr = run_request.stderr.as_ref().map(|_| ());

            let env_keys = run_request
                .env
                .as_ref()
                .map(|m| m.keys().map(Clone::clone).collect::<Vec<_>>());

            let n_to_downloads = run_request.to_downloads.as_ref().map(Vec::len);
            let n_to_uploads = run_request.to_uploads.as_ref().map(Vec::len);

            let mut wrapped_args = futures::future::join_all(
                run_request
                    .args
                    .into_iter()
                    .chain(iter::once(run_request.command))
                    .chain(run_request.stdout.into_iter())
                    .chain(run_request.stderr.into_iter())
                    .chain(run_request.env.into_iter().flat_map(|m| m.into_values()))
                    .chain(
                        run_request
                            .to_downloads
                            .into_iter()
                            .flat_map(|v| v.into_iter()),
                    )
                    .chain(
                        run_request
                            .to_uploads
                            .into_iter()
                            .flat_map(|v| v.into_iter()),
                    )
                    .map(|param| ContextStack::wrap_arg(self.ctx.clone(), param)),
            )
            .await;

            let cwd = run_request.cwd;

            n_to_downloads.map(|n_to_downloads| {
                (0..n_to_downloads)
                    .rev()
                    .map(|_| wrapped_args.pop().unwrap())
                    .collect::<Vec<_>>()
            });
            n_to_uploads.map(|n_to_uploads| {
                (0..n_to_uploads)
                    .rev()
                    .map(|_| wrapped_args.pop().unwrap())
                    .collect::<Vec<_>>()
            });

            let env = env_keys.map(|keys| {
                keys.into_iter()
                    .rev()
                    .map(|key| (key, wrapped_args.pop().unwrap()))
                    .collect()
            });
            let stderr = has_stderr.and_then(|_| wrapped_args.pop());
            let stdout = has_stdout.and_then(|_| wrapped_args.pop());
            let command = wrapped_args.pop().unwrap();
            let args = wrapped_args;

            RunSpec {
                command,
                args,
                cwd,
                env,
                stdout,
                stderr,
            }
        }

        async fn transform_response(&self, response: i32) -> RunResponse {
            RunResponse {
                return_code: response,
                exc: None,
            }
        }
    }

    pub(crate) struct UnpackAndDeserializeMiddle {}

    impl UnpackAndDeserializeMiddle {
        pub(crate) fn new() -> UnpackAndDeserializeMiddle {
            UnpackAndDeserializeMiddle {}
        }
    }

    #[async_trait]
    impl Middle<String, String, RunRequest, RunResponse> for UnpackAndDeserializeMiddle {
        async fn transform_request(&self, request: String) -> RunRequest {
            serde_json::from_str(request.as_str()).unwrap()
        }

        async fn transform_response(&self, response: RunResponse) -> String {
            serde_json::to_string(&response).unwrap()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[cfg(test)]
    mod test_client {
        use std::io::Write;

        use chain_ext::mongodb_gridfs::DatabaseExt;
        use fake::Fake;
        use tempfile::{tempdir, NamedTempFile};
        use test_utilities::docker;

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

            let req = RunRequest {
                command: Param::str("/bin/bash"),
                args: vec![
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
                ],
                stdout: Some(opath(
                    fake_stdout.path().to_str().unwrap(),
                    fake_stdout_content.clone(),
                )),
                stderr: Some(opath(fake_stderr.path().to_str().unwrap(), String::new())),
                cwd: None,
                env: None,
                to_downloads: None,
                to_uploads: None,
            };

            {
                let invoke_middle = client::ProxyInvokeMiddle::new(bucket.clone());
                let wrapped_req = invoke_middle.transform_request(req).await;

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
                for (wrapped_in_param, (_, content)) in
                    wrapped_in_params.iter().zip(in_params.iter())
                {
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

    #[cfg(test)]
    mod test_server {
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

            let req = RunRequest {
                command: Param::str("/bin/bash"),
                args: vec![
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
                ],
                stdout: Some(opath(
                    fake_stdout.path().to_str().unwrap(),
                    fake_stdout_content.clone(),
                )),
                stderr: Some(opath(fake_stderr.path().to_str().unwrap(), String::new())),
                cwd: None,
                env: None,
                to_downloads: None,
                to_uploads: None,
            };

            // mimic client upload input files
            for (param, content) in &in_params {
                param
                    .upload_from_string(bucket.clone(), content)
                    .await
                    .unwrap();
            }

            {
                let server_tempdir = tempdir().unwrap();
                let invoke_middle = server::ProxyInvokeMiddle::new(bucket.clone(), server_tempdir);
                let run_spec = invoke_middle.transform_request(req).await;

                assert_eq!(run_spec.command, "/bin/bash");
                assert_eq!(run_spec.args.first().unwrap(), "-c");

                let pat = regex::Regex::new(
                    "echo '(?P<content>.*)' && cat (?P<input>.*) > (?P<output>.*)",
                )
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
}
