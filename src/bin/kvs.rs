extern crate clap;
use clap::{App, Arg, crate_name, crate_version, crate_authors}; 
use std::process;

fn main() {
    let matches = App::new(crate_name!())
        .about("A kv store")
        .version(crate_version!())
        .author(crate_authors!())
        .subcommand(
            App::new("get")
                .about("Gets value according to the key")
                .arg(
                    Arg::new("KEY")
                        .required(true)
                        .index(1)
                )
            )
        .subcommand(
            App::new("set")
                .about("Sets key-value pair")
                .arg(
                    Arg::new("KEY")
                        .required(true)
                        .index(1)
                )
                .arg(
                    Arg::new("VALUE")
                        .required(true)
                        .index(2)
                ),
        )
        .subcommand(
            App::new("rm")
                .about("Remove key-value pair according to the key")
                .arg(
                    Arg::new("KEY")
                        .required(true)
                        .index(1)
                )
        )
        .get_matches();

    match matches.subcommand_name() {
        _ => {
            eprintln!("unimplemented");
            process::exit(1);
        }
    }
}
