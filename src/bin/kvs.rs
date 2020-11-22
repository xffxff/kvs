#[macro_use]
extern crate clap;
use clap::{App, load_yaml};
use std::process;

fn main() {
    let yaml = load_yaml!("cli.yml");
    let m = App::from(yaml).get_matches();  

    match m.subcommand_name() {
        _ => {
            eprintln!("unimplemented");
            process::exit(1);
        }
    }
}