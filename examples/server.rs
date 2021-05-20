// Example:
//
//     cargo run --features server --example server -- --bind '[::]:1883'

#[derive(Debug, structopt::StructOpt)]
struct Options {
    #[structopt(help = "Address of the MQTT server.", long)]
    bind: std::net::SocketAddr,
}

#[tokio::main(flavor = "current_thread")]
async fn main() {
    env_logger::Builder::from_env(env_logger::Env::new().filter_or(
        "MQTT3_LOG",
        "mqtt3=debug,mqtt3::logging=trace,subscriber=info",
    ))
    .init();

    let Options {
        bind,
    } = structopt::StructOpt::from_args();

    let server = mqtt3::Server::bind(bind).await.expect("couldn't bind server");
    let () = server.run().await.expect("server failed");
}
