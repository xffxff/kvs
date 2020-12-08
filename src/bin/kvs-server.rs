#[macro_use]
extern crate clap;
#[macro_use]
extern crate log;
use kvs::Result;
use structopt::StructOpt;
use std::net::SocketAddr;
use env_logger::Env;

arg_enum! {
    #[allow(non_camel_case_types)]
    #[derive(Debug)]
    enum Engine {
        kvs,
        sled
    }
}

#[derive(Debug, StructOpt)]
pub struct ApplicationArguments {
    #[structopt(long = "addr", default_value = "127.0.0.1:4000")]
    addr: SocketAddr,

    #[structopt(long = "engine", possible_values = &Engine::variants())]
    engine: Option<Engine>,
}

fn main() -> Result<()> {
    let env = Env::default()
        .filter_or("MY_LOG_LEVEL", "trace")
        .write_style_or("MY_LOG_STYLE", "always");
    env_logger::init_from_env(env);
    let opt = ApplicationArguments::from_args();

    info!("server version: {}", env!("CARGO_PKG_VERSION"));
    info!("IP:PORT {:?}", opt.addr);
    info!("Engine: {:?}", opt.engine);

    // let path = Path::new("./");
    // let mut kvs = KvStore::open(path)?;

    // match opt.command {
    //     Command::Set { ref key, ref value, addr: _} => {
    //         kvs.set(key.to_owned(), value.to_owned()).unwrap();
    //     }
    //     Command::Get { ref key, addr: _} => match kvs.get(key.to_owned()).unwrap() {
    //         Some(value) => println!("{}", value),
    //         None => println!("Key not found"),
    //     },
    //     Command::Remove { ref key, addr: _} => {
    //         kvs.remove(key.to_owned()).unwrap();
    //     }
    // }
    Ok(())
}
