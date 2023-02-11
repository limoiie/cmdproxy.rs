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
pub trait ArgGuard<P>: Send + Sync {
    async fn enter(&self) -> anyhow::Result<P>;
    async fn exit(&self) -> anyhow::Result<()> {
        Ok(())
    }
    async fn clean(&mut self) {}
}

#[async_trait]
pub trait GuardStack<P>: Send + Sync
where
    P: Send + Sync,
{
    async fn push_guard(&self, arg: Param) -> anyhow::Result<P> {
        let guard = self.guard_param(arg).await;
        let param = guard.enter().await;
        let guards = self.guards().lock().await;
        guards.borrow_mut().push(guard);
        param
    }

    async fn guard_param(&self, param: Param) -> Box<dyn ArgGuard<P>>;

    fn guards(&self) -> &ArcMtxRefCell<Vec<Box<dyn ArgGuard<P>>>>;

    async fn pop_all(&self) -> anyhow::Result<Vec<()>> {
        let mut guards = self.guards().lock().await;
        let guards = guards.get_mut();
        futures::future::join_all(guards.iter().map(|guard| guard.exit()))
            .await
            .into_iter()
            .collect()
    }

    async fn clean(&mut self) {
        let guards = self.guards().lock().await;
        let mut guards = guards.take();
        futures::future::join_all(guards.iter_mut().map(|guard| guard.clean())).await;
    }
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
    fn_guard: F,
) -> anyhow::Result<RunSpecification<P>>
where
    F: FnMut(Param) -> Fut,
    Fut: Future<Output = anyhow::Result<P>>,
{
    let env_keys = run_request
        .env
        .as_ref()
        .map(|m| m.keys().map(Clone::clone).collect::<Vec<_>>());

    let has_stdout = run_request.stdout.as_ref().map(|_| ());
    let has_stderr = run_request.stderr.as_ref().map(|_| ());

    let mut wrapped_args = futures::future::join_all(
        iter::empty()
            .chain(iter::once(run_request.command))
            .chain(run_request.env.into_iter().flat_map(|m| m.into_values()))
            .chain(run_request.stdout.into_iter())
            .chain(run_request.stderr.into_iter())
            .chain(run_request.args.into_iter())
            .map(fn_guard),
    )
    .await
    .into_iter()
    .collect::<anyhow::Result<LinkedList<_>>>()?;

    let cwd = run_request.cwd;

    let command = wrapped_args.pop_front().unwrap();
    let env = env_keys.map(|keys| {
        keys.into_iter()
            .rev()
            .map(|key| (key, wrapped_args.pop_front().unwrap()))
            .collect()
    });
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
