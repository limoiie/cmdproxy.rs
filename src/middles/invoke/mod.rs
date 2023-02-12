use std::cell::RefCell;
use std::collections::{HashMap, LinkedList};
use std::future::Future;
use std::iter;
use std::sync::Arc;

use celery::export::async_trait;
use tokio::sync::Mutex;

use crate::middles::Middle;
use crate::protocol::{RunResponse, RunSpecification};

pub mod client_end;
pub mod server_end;

#[async_trait]
pub trait ArgGuard<P, D>: Send + Sync
where
    D: Send + Sync,
{
    async fn enter(&self, data: &ArcMtxRefCell<D>) -> anyhow::Result<P>;
    async fn exit(&self, _: &ArcMtxRefCell<D>) -> anyhow::Result<()> {
        Ok(())
    }
}

#[async_trait]
pub trait InvokeMiddle<PA, PB>: Send + Sync
where
    PA: Send + Sync,
    PB: Send + Sync,
{
    async fn push_guard(&self, param: PA, key: Option<String>) -> anyhow::Result<PB>;
    async fn pop_all_guards(&self) -> anyhow::Result<Vec<()>>;
}

#[async_trait]
impl<PA, PB, M> Middle<RunSpecification<PA>, RunResponse, RunSpecification<PB>, RunResponse> for M
where
    PA: Send + Sync + 'static,
    PB: Send + Sync,
    M: InvokeMiddle<PA, PB>,
{
    async fn transform_request(
        &self,
        request: RunSpecification<PA>,
    ) -> anyhow::Result<RunSpecification<PB>> {
        guard_run_args(request, |param, key| self.push_guard(param, key)).await
    }

    async fn transform_response(
        &self,
        response: anyhow::Result<RunResponse>,
    ) -> anyhow::Result<RunResponse> {
        self.pop_all_guards().await?;
        response
    }
}

#[async_trait]
pub trait GuardStackData<PA, PB>: Sized + Send + Sync
where
    PA: Send + Sync + 'static,
    PB: Send + Sync,
{
    fn pass_env(&mut self, key: String, val: &PB);

    async fn push_guard(
        data: &ArcMtxRefCell<Self>,
        arg: PA,
        key: Option<String>,
    ) -> anyhow::Result<PB> {
        let param = {
            let guard = {
                let data = data.lock().await;
                let data = data.borrow();
                data.guard_param(arg)
            };
            let param = guard.enter(data).await?;
            let data = data.lock().await;
            let mut data = data.borrow_mut();
            data.guards_mut().push(guard);
            param
        };
        if let Some(key) = key {
            let data = data.lock().await;
            let mut data = data.borrow_mut();
            data.pass_env(key, &param);
        };
        Ok(param)
    }

    fn guard_param(&self, param: PA) -> Box<dyn ArgGuard<PB, Self>>;

    fn guards(&self) -> &Vec<Box<dyn ArgGuard<PB, Self>>>;

    fn guards_mut(&mut self) -> &mut Vec<Box<dyn ArgGuard<PB, Self>>>;
}

#[async_trait]
pub trait GuardStack<PA, PB, D>: Send + Sync
where
    PA: Send + Sync + 'static,
    PB: Send + Sync,
    D: GuardStackData<PA, PB>,
{
    async fn push_guard(&self, arg: PA, key: Option<String>) -> anyhow::Result<PB> {
        GuardStackData::push_guard(self.data(), arg, key).await
    }

    async fn pop_all_guards(&self) -> anyhow::Result<Vec<()>> {
        let guards = self.guards().await;
        futures::future::join_all(guards.iter().map(|guard| guard.exit(self.data())))
            .await
            .into_iter()
            .collect()
    }

    async fn guards(&self) -> Vec<Box<dyn ArgGuard<PB, D>>> {
        let data = self.data().lock().await;
        let mut data = data.borrow_mut();
        let guards = data.guards_mut();
        let mut out = vec![];
        while let Some(guard) = guards.pop() {
            out.push(guard)
        }
        out
    }

    fn data(&self) -> &ArcMtxRefCell<D>;
}

pub async fn guard_hashmap_args<PA, PB, F, Fut>(
    args: &HashMap<String, PA>,
    fn_guard: F,
) -> anyhow::Result<HashMap<String, PB>>
where
    PA: Clone,
    F: FnMut(PA) -> Fut,
    Fut: Future<Output = anyhow::Result<PB>>,
{
    let args = args
        .keys()
        .map(Clone::clone)
        .zip(
            futures::future::join_all(args.values().map(Clone::clone).map(fn_guard))
                .await
                .into_iter()
                .collect::<anyhow::Result<Vec<_>>>()?
                .into_iter(),
        )
        .collect();

    Ok(args)
}

async fn guard_run_args<PA, PB, F, Fut>(
    run_request: RunSpecification<PA>,
    mut fn_guard: F,
) -> anyhow::Result<RunSpecification<PB>>
where
    F: FnMut(PA, Option<String>) -> Fut,
    Fut: Future<Output = anyhow::Result<PB>>,
{
    let cwd = run_request.cwd;
    let env = if let Some(env) = run_request.env {
        let mut wrapped_env = HashMap::new();
        for (key, arg) in env.into_iter() {
            let wrapped_arg = fn_guard(arg, Some(key.clone())).await?;
            wrapped_env.insert(key, wrapped_arg);
        }
        Some(wrapped_env)
    } else {
        None
    };

    let has_stdout = run_request.stdout.as_ref().map(|_| ());
    let has_stderr = run_request.stderr.as_ref().map(|_| ());

    let mut wrapped_args = futures::future::join_all(
        iter::empty()
            .chain(iter::once(run_request.command))
            .chain(run_request.stdout.into_iter())
            .chain(run_request.stderr.into_iter())
            .chain(run_request.args.into_iter())
            .map(|param| fn_guard(param, None)),
    )
    .await
    .into_iter()
    .collect::<anyhow::Result<LinkedList<_>>>()?;

    let command = wrapped_args.pop_front().unwrap();
    let stdout = has_stdout.and_then(|_| wrapped_args.pop_front());
    let stderr = has_stderr.and_then(|_| wrapped_args.pop_front());
    let args = wrapped_args.into_iter().collect();

    Ok(RunSpecification::<PB> {
        command,
        args,
        cwd,
        env,
        stdout,
        stderr,
    })
}

pub type ArcMtxRefCell<T> = Arc<Mutex<RefCell<T>>>;

#[cfg(test)]
mod tests {
    use chain_ext::mongodb_gridfs::DatabaseExt;
    use tempfile::tempdir;
    use test_utilities::docker;

    use crate::middles::invoke::server_end::Config;
    use crate::params::Param;
    use crate::protocol::RunRequest;

    use super::*;

    #[tokio::test]
    async fn test_guard_run_args_resolve_passed_env() {
        let container = docker::Builder::new("mongo")
            .name("cmdproxy-test-guard_run_args")
            .bind_port_as_default(Some("0"), "27017")
            .build_disposable()
            .await;

        let bucket = mongodb::Client::with_uri_str(container.url())
            .await
            .unwrap()
            .database("cmdproxy-test-db")
            .bucket(None);

        let fake_password = "fake password";
        let conf = Config {
            command_palette: HashMap::<String, String>::new(),
        };

        let req = RunRequest::builder()
            .command(Param::str("/bin/sh"))
            .args(vec![Param::env("PASSWORD")])
            .stdout(Param::env("PASSWORD"))
            .stderr(Param::format(
                "{pwd}",
                HashMap::from([("pwd", Param::env("PASSWORD"))]),
            ))
            .env(HashMap::from([(
                "PASSWORD".to_owned(),
                Param::str(fake_password),
            )]))
            .build();

        let server_tempdir = tempdir().unwrap();
        let middle = server_end::MiddleImpl::new(bucket.clone(), server_tempdir, conf);
        let spec = middle.transform_request(req).await;

        assert!(spec.is_ok());
        let spec = spec.unwrap();
        assert_eq!(spec.args, vec![fake_password.to_owned()]);
        assert_eq!(spec.stdout, Some(fake_password.to_owned()));
        assert_eq!(spec.stderr, Some(fake_password.to_owned()));
    }
}
