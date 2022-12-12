use std::sync::Arc;

use celery::backend::MongoDbBackend;
use celery::broker::RedisBroker;
use celery::prelude::*;
use celery::result::BaseResult;
use celery::Celery;

use crate::apply_middles;
use crate::configs::CmdProxyClientConf;
use crate::middles::{client, Middle};
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
            // todo: how to specify queues?
            task_routes = ["*" => "celery"],
        )
        .await
        .unwrap();

        Client { conf, app }
    }

    pub async fn run(&self, run_request: RunRequest) -> i32 {
        let app = self.app.clone();
        let bucket = self.conf.cloud.grid_fs().await;

        let proxy_run = |serialized: String| async {
            // todo: how to specify queues?
            let sig = run::new(serialized).with_queue("sh");
            app.send_task(sig)
                .await
                .unwrap()
                .wait(None)
                .await
                // todo: how to handle the errors?
                .unwrap()
                .unwrap()
        };

        let res = apply_middles!(run_request,
            @client::ProxyInvokeMiddle::new(bucket)
            @client::PackAndSerializeMiddle::new()
            => proxy_run
        );
        res.return_code
    }
}
