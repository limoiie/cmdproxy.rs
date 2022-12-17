use std::sync::Arc;

use celery::backend::MongoDbBackend;
use celery::broker::RedisBroker;
use celery::prelude::*;
use celery::result::BaseResult;
use celery::task::Signature;
use celery::Celery;
use log::debug;

use crate::apply_middles;
use crate::configs::CmdProxyClientConf;
use crate::middles::{client, Middle};
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
        let app = self.app.clone();
        let bucket = self.conf.cloud.grid_fs().await;
        let queue = queue
            .or_else(|| match &run_request.command {
                Param::EnvParam { name } => Some(name.clone()),
                _ => None,
            })
            .expect("You must either explicitly specify the queue, or wrap command as EnvParam!");

        let proxy_run = |serialized: String| async {
            debug!("Sending RunRequest to queue `{queue}'...");

            let sig: Signature<_> = run::new(serialized).with_queue(queue.as_str());
            Ok(app.send_task(sig).await.unwrap().wait(None).await??)
        };

        let res = apply_middles!(
            run_request,
            >=< client::ProxyInvokeMiddle::new(bucket),
            >=< client::PackAndSerializeMiddle::new(),
            >>= proxy_run
        );
        res.map(|r| r.return_code)
    }
}
