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
    use crate::protocol::RunResponse;

    use super::*;

    struct StrGuard {
        value: String,
    }

    struct EnvGuard {
        name: String,
    }

    struct InCloudFileGuard {
        tempdir: Arc<TempDir>,
        bucket: GridFSBucket,
        param: Param,
    }

    struct OutCloudFileGuard {
        tempdir: Arc<TempDir>,
        temppath: Arc<Mutex<RefCell<String>>>,
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
            let temppath = tempfile::Builder::new()
                .tempfile_in(self.tempdir.as_ref().path())
                .unwrap()
                .into_temp_path();

            self.param
                .download(self.bucket.clone(), temppath.to_path_buf())
                .await
                .unwrap();

            temppath.to_str().unwrap().to_string()
        }
    }

    #[async_trait]
    impl ArgGuard for OutCloudFileGuard {
        async fn enter(&self) -> String {
            let temppath = tempfile::Builder::new()
                .tempfile_in(self.tempdir.as_ref().path())
                .unwrap()
                .into_temp_path();

            let mutex = self.temppath.lock().await;
            *mutex.borrow_mut() = temppath.to_str().unwrap().to_string();

            temppath.to_str().unwrap().to_string()
        }

        async fn exit(&mut self) {
            let mutex = self.temppath.lock().await;
            let filepath = mutex.borrow().to_string();

            self.param
                .upload(self.bucket.clone(), filepath)
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
        tempdir: Arc<TempDir>,
        bucket: GridFSBucket,
        guards: RefCell<Vec<Box<dyn ArgGuard>>>,
    }

    impl ContextStack {
        pub async fn wrap_arg(ctx: Arc<Mutex<ContextStack>>, arg: Param) -> String {
            let bucket = ctx.lock().await.bucket.clone();

            let guard: Box<dyn ArgGuard> = match arg {
                Param::StrParam { value } => Box::new(StrGuard { value }),
                Param::EnvParam { name } => Box::new(EnvGuard { name }),
                Param::FormatParam { tmpl, args } => Box::new(FormatGuard {
                    tmpl,
                    args,
                    ctx: ctx.clone(),
                }),
                param @ Param::InCloudFileParam { .. } => Box::new(InCloudFileGuard {
                    tempdir: ctx.lock().await.tempdir.clone(),
                    bucket,
                    param,
                }),
                param @ Param::OutCloudFileParam { .. } => Box::new(OutCloudFileGuard {
                    tempdir: ctx.lock().await.tempdir.clone(),
                    temppath: Arc::new(Mutex::new(RefCell::new("".to_string()))),
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
                    tempdir: Arc::new(tempdir),
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
