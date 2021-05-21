// Example:
//
// tokio transport:
//     cargo run --features server,transport-tokio --example server -- --bind '[::]:1883'

mod common;

#[derive(Debug, structopt::StructOpt)]
struct Options {
    #[structopt(help = "Address of the MQTT server.", long)]
    bind: std::net::SocketAddr,
}

#[cfg(feature = "transport-tokio")]
fn main() {
    let runtime =
        tokio::runtime::Builder::new_current_thread()
        .enable_io()
        .build().expect("could not create runtime");

    let local_set = tokio::task::LocalSet::new();

    let () = local_set.block_on(&runtime, tokio::task::unconstrained(main_inner()));
}

async fn main_inner() {
    env_logger::Builder::from_env(env_logger::Env::new().filter_or(
        "MQTT3_LOG",
        "mqtt3=debug,mqtt3::io=trace,server=info",
    ))
    .init();

    let Options {
        bind,
    } = structopt::StructOpt::from_args();

    #[cfg(feature = "transport-tokio")]
    let listener = common::tokio::Listener::bind(bind).await.expect("bind failed");

    let () = mqtt3::server::run(listener).await.expect("server failed");
}
