#![allow(non_upper_case_globals)]

use anyhow::Result;
use clap::Parser;

#[tokio::main]
async fn main() -> Result<()> {
    cmdproxy::app(cmdproxy::Cli::parse()).await
}
