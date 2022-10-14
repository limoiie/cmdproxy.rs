#![allow(non_upper_case_globals)]

use anyhow::Result;
use clap::Parser;

#[tokio::main]
async fn main() -> Result<()> {
    cmd_proxy::app(cmd_proxy::Cli::parse()).await
}
