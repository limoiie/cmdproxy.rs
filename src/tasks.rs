use celery::prelude::TaskResult;
use once_cell::sync::OnceCell;

use crate::configs::CmdProxyServerConf;
use crate::server::Server;

pub static SERVER_CONF: OnceCell<CmdProxyServerConf> = OnceCell::new();

#[celery::task]
pub async fn run(serialized_run_request: String) -> TaskResult<String> {
    let conf = SERVER_CONF.get().unwrap().clone();
    let server = Server::new(conf).await;
    let serialized_response = server.run(serialized_run_request).await;
    Ok(serialized_response)
}
