use std::collections::HashMap;
use std::io::Write;

use chain_ext::option::OptionExt;
use clap::Parser;
use fake::Fake;
use tempfile::{tempdir, NamedTempFile};

use cmdproxy::configs::{CmdProxyClientConf, CmdProxyClientConfFile};
use cmdproxy::params::Param;
use cmdproxy::protocol::RunRequest;

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
pub struct Cli {
    /// Uri to the redis broker
    #[arg(short, long)]
    redis_url: Option<String>,

    /// Uri to the mongo remote-fs
    #[arg(short, long)]
    mongo_url: Option<String>,

    /// Name of database where stores the remote-fs
    #[arg(long)]
    mongo_dbname: Option<String>,
}

#[tokio::main]
async fn main() {
    let fake_workspace = tempdir().unwrap();

    let mut fake_input = NamedTempFile::new_in(fake_workspace.path()).unwrap();
    let fake_output = NamedTempFile::new_in(fake_workspace.path()).unwrap();

    let fake_stdout = NamedTempFile::new_in(fake_workspace.path()).unwrap();
    let fake_stderr = NamedTempFile::new_in(fake_workspace.path()).unwrap();

    let fake_input_content = (30..50).fake::<String>();
    let fake_stdout_content = (30..50).fake::<String>();

    fake_input.write_all(fake_input_content.as_bytes()).unwrap();

    let conf = parse_client_conf();

    let req = RunRequest::builder()
        .command(Param::cmd_path("/bin/sh"))
        .args(vec![
            Param::str("-c"),
            Param::format(
                "echo '{content}' && cat {input} > {output}",
                HashMap::from([
                    ("content", Param::str(fake_stdout_content.clone())),
                    ("input", Param::ipath(fake_input.path().to_str().unwrap())),
                    ("output", Param::opath(fake_output.path().to_str().unwrap())),
                ]),
            ),
        ])
        .stdout(Param::opath(fake_stdout.path().to_str().unwrap()))
        .stderr(Param::opath(fake_stderr.path().to_str().unwrap()))
        .build();

    println!("running through the proxy...");
    let client = cmdproxy::client::Client::new(conf).await;
    let response = client.run(req, Some("sh".to_string())).await;

    assert_eq!(0, response.unwrap());

    println!(
        "received stdout: {}",
        tokio::fs::read_to_string(fake_stdout.path()).await.unwrap()
    );
    println!(
        "received stderr: {}",
        tokio::fs::read_to_string(fake_stderr.path()).await.unwrap()
    );

    println!("checking stdout output...");
    let stdout_content = tokio::fs::read_to_string(fake_stdout.path()).await.unwrap();
    assert_eq!(fake_stdout_content + "\n", stdout_content);

    println!("checking normal output...");
    let output_content = tokio::fs::read_to_string(fake_output.path()).await.unwrap();
    assert_eq!(fake_input_content, output_content);

    println!("bingo!");
}

fn parse_client_conf() -> CmdProxyClientConf {
    let cli: Cli = Cli::parse();

    let redis_url = cli
        .redis_url
        .or_ok(std::env::var("CMDPROXY_REDIS_URL"))
        .or_wrap("redis://localhost:6379/".to_owned())
        .unwrap();

    let mongo_url = cli
        .mongo_url
        .or_ok(std::env::var("CMDPROXY_MONGO_URL"))
        .or_wrap("mongodb://localhost:27017/".to_owned())
        .unwrap();

    let mongo_dbname = cli
        .mongo_dbname
        .or_ok(std::env::var("CMDPROXY_MONGO_DBNAME"))
        .or_wrap("cmdproxy-db".to_owned())
        .unwrap();

    let conf = CmdProxyClientConf::new(CmdProxyClientConfFile {
        redis_url: redis_url.clone(),
        mongo_url: mongo_url.clone(),
        mongo_dbname: mongo_dbname.clone(),
    });

    println!("redis run on: {}", redis_url);
    println!("mongo run on: {}", mongo_url);
    println!("mongo dbname: {}", mongo_dbname);

    conf
}
