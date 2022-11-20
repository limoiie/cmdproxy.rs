use std::sync::Arc;

use celery::prelude::*;
use celery::task::Signature;
use celery::Celery;
use chain_ext::mongodb_gridfs::DatabaseExt;
use chain_ext::option::OptionExt;
use clap::Parser;
use fake::Fake;
use mongodb::Client;
use mongodb_gridfs_ext::bucket::common::GridFSBucketExt;
use test_utilities::fs::TempFileKind::Text;
use test_utilities::gridfs;

use cmd_proxy::run_request::RunRequest;
use cmd_proxy::tasks::run;

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
    mongodb_name: Option<String>,
}

#[tokio::main]
async fn main() {
    let cli: Cli = Cli::parse();

    let redis_url = cli
        .redis_url
        .or_ok(std::env::var("REDIS_URI"))
        .or_wrap("redis://localhost:6379/".to_owned())
        .unwrap();

    let mongo_url = cli
        .mongo_url
        .or_ok(std::env::var("MONGO_URI"))
        .or_wrap("mongodb://localhost:27017/".to_owned())
        .unwrap();

    let mongodb_name = cli
        .mongodb_name
        .or_ok(std::env::var("MONGODB_NAME"))
        .or_wrap("testdb".to_owned())
        .unwrap();

    println!("redis run on: {}", redis_url);
    println!("mongo run on: {}", mongo_url);
    println!("mongo dbname: {}", mongodb_name);

    let app: Arc<Celery<RedisBroker, RedisBackend>> = celery::app!(
        broker = RedisBroker { redis_url.clone() },
        backend = RedisBackend { redis_url.clone() },
        tasks = [run],
        task_routes = ["*" => "celery"],
    )
    .await
    .unwrap();

    let bucket = Client::with_uri_str(mongo_url)
        .await
        .unwrap()
        .database(&mongodb_name)
        .bucket(None);

    // prepare the fake cloud input
    let faker = gridfs::TempFileFaker::with_bucket(bucket.clone())
        .kind(Text)
        .include_content(true);
    let fake_input = faker.fake::<gridfs::TempFile>();

    let input_link = fake_input.filename.unwrap();
    let output_link = (10..20).fake::<String>();
    let stdout_link = (10..20).fake::<String>();
    let stdout_content = "hello";

    println!("filename of cloud input: {}", input_link);
    println!("filename of cloud output: {}", output_link);

    let req = RunRequest {
        command: "sh".to_string(),
        args: vec![
            format!("-c"),
            format!(
                "echo {} && cat <#:i>{}</> > <#:o>{}</>",
                stdout_content, input_link, output_link
            ),
        ],
        stdout: Some(stdout_link.clone()),
        ..RunRequest::default()
    };
    let serialized_req = serde_json::to_string(&req).unwrap();

    let sig: Signature<_> = run::new(serialized_req).with_queue("sh");
    let res = app
        .send_task(sig.with_queue("sh"))
        .await
        .unwrap()
        .wait(None)
        .await;

    println!("returns: {:?}", res);

    // read the cloud output from cloud
    println!("checking normal output...");
    let output_content = bucket
        .read_as_bytes(bucket.id(&output_link).await.unwrap())
        .await
        .unwrap();
    assert_eq!(fake_input.content.unwrap(), output_content);

    println!("checking stdout output...");
    let output_stdout_content = bucket
        .read_as_bytes(bucket.id(&stdout_link).await.unwrap())
        .await
        .unwrap();
    assert_eq!(
        (stdout_content.to_string() + "\n").as_bytes(),
        output_stdout_content.as_slice()
    );

    println!("bingo!");
}
