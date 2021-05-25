// Example:
//
//     cargo run --features client --example subscriber -- --topic-filter foo --qos 1

use futures_util::StreamExt;

mod common;

#[derive(Debug, structopt::StructOpt)]
struct Options {
    /// Address of the MQTT server.
    #[structopt(long, default_value = "[::1]:1883")]
    server: std::net::SocketAddr,

    /// Client ID used to identify this application to the server. If not given, a server-generated ID will be used.
    #[structopt(long)]
    client_id: Option<mqtt3::proto::ByteStr>,

    /// Username used to authenticate with the server, if any.
    #[structopt(long)]
    username: Option<mqtt3::proto::ByteStr>,

    /// Password used to authenticate with the server, if any.
    #[structopt(long)]
    password: Option<mqtt3::proto::ByteStr>,

    /// Maximum back-off time between reconnections to the server, in seconds.
    #[structopt(long, default_value = "30", parse(try_from_str = common::duration_from_secs_str))]
    max_reconnect_back_off: std::time::Duration,

    /// Keep-alive time advertised to the server, in seconds.
    #[structopt(long, default_value = "5", parse(try_from_str = common::duration_from_secs_str))]
    keep_alive: std::time::Duration,

    /// The topic filter to subscribe to.
    #[structopt(long)]
    topic_filter: mqtt3::proto::ByteStr,

    /// The QoS with which to subscribe to the topic.
    #[structopt(long, parse(try_from_str = common::qos_from_str))]
    qos: mqtt3::proto::QoS,
}

#[tokio::main(flavor = "current_thread")]
async fn main() {
    let Options {
        server,
        client_id,
        username,
        password,
        max_reconnect_back_off,
        keep_alive,
        topic_filter,
        qos,
    } = common::init("subscriber");

    let mut client = mqtt3::Client::new(
        client_id,
        username,
        None,
        move || {
            let password = password.clone();
            Box::pin(async move {
                let (stream, sink) = common::transport::tokio::connect(server).await?;
                Ok::<_, std::io::Error>((stream, sink, password))
            })
        },
        max_reconnect_back_off,
        keep_alive,
    );

    let mut shutdown_handle = client
        .shutdown_handle()
        .expect("couldn't get shutdown handle");
    tokio::spawn(async move {
        let () = tokio::signal::ctrl_c()
            .await
            .expect("couldn't get Ctrl-C notification");
        let result = shutdown_handle.shutdown().await;
        let () = result.expect("couldn't send shutdown notification");
    });

    let mut update_subscription_handle = client
        .update_subscription_handle()
        .expect("couldn't get subscription update handle");
    tokio::spawn(async move {
        let result = update_subscription_handle
            .subscribe(mqtt3::proto::SubscribeTo { topic_filter, qos })
            .await;
        if let Err(err) = result {
            panic!("couldn't update subscription: {}", err);
        }
    });

    let mut packet_stats: common::PacketStats = Default::default();

    while let Some(event) = client.next().await {
        let event = event.unwrap();

        if let mqtt3::Event::Publication(publication) = event {
            match std::str::from_utf8(&publication.payload) {
                Ok(s) => log::debug!(
                    "Received publication: {:?} {:?} {:?}",
                    publication.topic_name,
                    s,
                    publication.qos,
                ),
                Err(_) => log::debug!(
                    "Received publication: {:?} {:?} {:?}",
                    publication.topic_name,
                    publication.payload,
                    publication.qos,
                ),
            }

            packet_stats.count(1);
        }
    }
}
