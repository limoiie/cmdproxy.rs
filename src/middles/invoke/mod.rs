use std::cell::RefCell;
use std::collections::{HashMap, LinkedList};
use std::future::Future;
use std::iter;
use std::sync::Arc;

use celery::export::async_trait;
use tokio::sync::Mutex;

use crate::params::Param;
use crate::protocol::{RunRequest, RunSpecification};

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
pub trait GuardStackData<P>: Sized + Send + Sync
where
    P: Send + Sync,
{
    fn pass_env(&mut self, key: String, val: &P);

    async fn push_guard(
        data: &ArcMtxRefCell<Self>,
        arg: Param,
        key: Option<String>,
    ) -> anyhow::Result<P> {
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

    fn guard_param(&self, param: Param) -> Box<dyn ArgGuard<P, Self>>;

    fn guards(&self) -> &Vec<Box<dyn ArgGuard<P, Self>>>;

    fn guards_mut(&mut self) -> &mut Vec<Box<dyn ArgGuard<P, Self>>>;
}

#[async_trait]
pub trait GuardStack<P, D>: Send + Sync
where
    P: Send + Sync,
    D: GuardStackData<P>,
{
    async fn push_guard(&self, arg: Param, key: Option<String>) -> anyhow::Result<P> {
        GuardStackData::push_guard(self.data(), arg, key).await
    }

    async fn pop_all_guards(&self) -> anyhow::Result<Vec<()>> {
        let guards = self.guards().await;
        futures::future::join_all(guards.iter().map(|guard| guard.exit(self.data())))
            .await
            .into_iter()
            .collect()
    }

    async fn guards(&self) -> Vec<Box<dyn ArgGuard<P, D>>> {
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

pub async fn guard_hashmap_args<P, F, Fut>(
    args: &HashMap<String, Param>,
    fn_guard: F,
) -> anyhow::Result<HashMap<String, P>>
where
    F: FnMut(Param) -> Fut,
    Fut: Future<Output = anyhow::Result<P>>,
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

pub async fn guard_run_args<P, F, Fut>(
    run_request: RunRequest,
    mut fn_guard: F,
) -> anyhow::Result<RunSpecification<P>>
where
    F: FnMut(Param, Option<String>) -> Fut,
    Fut: Future<Output = anyhow::Result<P>>,
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

    Ok(RunSpecification::<P> {
        command,
        args,
        cwd,
        env,
        stdout,
        stderr,
    })
}

pub type ArcMtxRefCell<T> = Arc<Mutex<RefCell<T>>>;
