// Example:
//
// smol transport:
//     cargo run --features server,transport-smol --example server -- --bind '[::]:1883'
//
// tokio transport:
//     cargo run --features server,transport-tokio --example server -- --bind '[::]:1883'

mod common;

#[derive(Debug, structopt::StructOpt)]
struct Options {
    /// Address of the MQTT server.
    #[structopt(long, default_value = "[::]:1883")]
    bind: std::net::SocketAddr,
}

#[cfg(feature = "transport-smol")]
fn main() {
    let bind = init();
    let listener = smol::block_on(common::transport::smol::Listener::bind(bind)).expect("bind failed");

    let () = smol::block_on(mqtt3::server::run(listener)).expect("server failed");
}

#[cfg(feature = "transport-tokio")]
fn main() {
    let runtime =
        tokio::runtime::Builder::new_current_thread()
        .enable_io()
        .enable_time()
        .build().expect("could not create runtime");
    let local_set = tokio::task::LocalSet::new();

    let bind = init();
    let listener = local_set.block_on(&runtime, common::transport::tokio::Listener::bind(bind)).expect("bind failed");

    let () = local_set.block_on(&runtime, tokio::task::unconstrained(mqtt3::server::run(listener))).expect("server failed");
}

fn init() -> std::net::SocketAddr {
    let Options {
        bind,
    } = common::init("server");
    bind
}
