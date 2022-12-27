use std::sync::Arc;

use anyhow::anyhow;
use celery::backend::MongoDbBackend;
use celery::broker::RedisBroker;
use celery::prelude::*;
use celery::result::BaseResult;
use celery::task::Signature;
use celery::Celery;
use log::debug;

use crate::apply_middles;
use crate::configs::CmdProxyClientConf;
use crate::middles::{invoke, serde, Middle};
use crate::params::Param;
use crate::protocol::RunRequest;
use crate::tasks::run;

pub struct Client {
    conf: CmdProxyClientConf,
    app: Arc<Celery<RedisBroker, MongoDbBackend>>,
}

impl Client {
    pub async fn new(conf: CmdProxyClientConf) -> Client {
        let app: Arc<Celery<RedisBroker, MongoDbBackend>> = celery::app!(
            broker = RedisBroker { conf.celery.broker_url.clone() },
            backend = MongoDbBackend { conf.celery.backend_url.clone() },
            tasks = [run],
            task_routes = ["*" => "celery"],
        )
        .await
        .unwrap();

        Client { conf, app }
    }

    pub async fn run(&self, run_request: RunRequest, queue: Option<String>) -> anyhow::Result<i32> {
        let queue = match &run_request.command {
            Param::CmdNameParam { name } => queue.unwrap_or_else(|| name.clone()),
            Param::CmdPathParam { .. } => queue.ok_or_else(|| {
                anyhow!("Queue should be specified when command is instance of CmdPathParam")
            })?,
            param => {
                return Err(anyhow!(
                    "Expect command in type of CmdNameParam or CmdPathParam, got {:#?}",
                    param
                ))
            }
        };

        let app = self.app.clone();
        let bucket = self.conf.cloud.grid_fs().await;

        let proxy_run = |serialized: String| async {
            debug!("Sending RunRequest to queue `{queue}'...");

            let sig: Signature<_> = run::new(serialized).with_queue(queue.as_str());
            Ok(app.send_task(sig).await.unwrap().wait(None).await??)
        };

        let res = apply_middles!(
            run_request,
            >=< [ invoke::client_end::MiddleImpl::new(bucket) ]
            >=< [ serde::client_end::MiddleImpl::new() ]
            >>= proxy_run
        );
        res.map(|r| r.return_code)
    }
}
