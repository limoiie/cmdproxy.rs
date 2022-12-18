use std::fs::File;
use std::process::Stdio;

use log::debug;
use tempfile::tempdir;

use crate::apply_middles;
use crate::configs::CmdProxyServerConf;
use crate::middles::{server, Middle};
use crate::protocol::RunSpec;

pub struct Server {
    conf: CmdProxyServerConf,
}

impl Server {
    pub(crate) async fn new(conf: CmdProxyServerConf) -> Server {
        Server { conf }
    }

    pub(crate) async fn run(&self, serialized_run_request: String) -> String {
        let workspace = tempdir().unwrap();
        let bucket = self.conf.cloud.grid_fs().await;

        let real_run = |run_spec: RunSpec| async move {
            debug!("Running command with spec as:\n{:#?}", run_spec);

            let stdout = run_spec
                .stdout
                .as_ref()
                .map(|path| Stdio::from(File::create(path).unwrap()))
                .unwrap_or_else(Stdio::inherit);
            let stderr = run_spec
                .stderr
                .as_ref()
                .map(|path| Stdio::from(File::create(path).unwrap()))
                .unwrap_or_else(Stdio::inherit);

            let mut command = std::process::Command::new(run_spec.command);
            let st = command
                .args(&run_spec.args)
                .stdout(stdout)
                .stderr(stderr)
                .current_dir(run_spec.cwd.unwrap_or_else(|| ".".to_owned()))
                .envs(run_spec.env.unwrap_or_default())
                .status();
            Ok(st?.code().unwrap_or(0))
        };

        let res = apply_middles!(
            serialized_run_request,
            >=< [ server::UnpackAndDeserializeMiddle::new() ]
            >=< [ server::ProxyInvokeMiddle::new(bucket, workspace) ]
            >>= real_run
        );
        res.expect("Unreachable: please embedding all the errors into serialization!")
    }
}
