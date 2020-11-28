use std::process;
use structopt::StructOpt;
use kvs::Result;

#[derive(Debug, StructOpt)]
pub struct Set {
    pub key: String,
    pub value: String,
}

#[derive(Debug, StructOpt)]
pub struct Get {
    pub key: String,
}

#[derive(Debug, StructOpt)]
pub struct Remove {
    pub key: String,
}

#[derive(Debug, StructOpt)]
pub enum Command {
    #[structopt(name = "set", about = "Stores a key/value pair")]
    Set(Set),
    #[structopt(name = "get", about = "Gets value according to the key")]
    Get(Get),
    #[structopt(name = "rm", about = "Removes key/value pair according to the key")]
    Remove(Remove),
}

#[derive(Debug, StructOpt)]
pub struct ApplicationArguments {
    #[structopt(subcommand)]
    pub command: Command,
}

fn main() -> Result<()> {
    ApplicationArguments::from_args();
    eprintln!("unimplemented");
    process::exit(1);
}