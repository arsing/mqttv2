// Example:
//
//     cargo run --features client --example publisher -- --publish-frequency 1000 --topic foo --qos 1 --payload 'hello, world'

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

    /// How often to publish to the server, in milliseconds.
    #[structopt(long, default_value = "1000", parse(try_from_str = duration_from_millis_str))]
    publish_frequency: std::time::Duration,

    /// The topic of the publications.
    #[structopt(long)]
    topic: mqtt3::proto::ByteStr,

    /// The QoS of the publications.
    #[structopt(long, parse(try_from_str = common::qos_from_str))]
    qos: mqtt3::proto::QoS,

    /// The payload of the publications.
    #[structopt(long)]
    payload: String,
}

#[tokio::main]
async fn main() {
    let Options {
        server,
        client_id,
        username,
        password,
        max_reconnect_back_off,
        keep_alive,
        publish_frequency,
        topic,
        qos,
        payload,
    } = common::init("publisher");

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

    let payload: bytes::Bytes = payload.into();

    let publish_handle = client
        .publish_handle()
        .expect("couldn't get publish handle");
    tokio::spawn(async move {
        let mut interval =
            if publish_frequency.as_nanos() == 0 {
                None
            }
            else {
                Some(tokio::time::interval(publish_frequency))
            };
        loop {
            if let Some(interval) = interval.as_mut() {
                interval.tick().await;
            }

            let topic = topic.clone();
            log::debug!("Publishing to {} ...", topic);

            let mut publish_handle = publish_handle.clone();
            let payload = payload.clone();

            let f = async move {
                let result = publish_handle
                    .publish(mqtt3::proto::Publication {
                        topic_name: topic.clone(),
                        qos,
                        retain: false,
                        payload,
                    })
                    .await;
                let () = result.expect("couldn't publish");
                log::debug!("Published to {}", topic);
                Ok::<_, ()>(())
            };
            if interval.is_some() {
                tokio::spawn(f);
            }
            else {
                let _ = f.await;
            }
        }
    });

    while let Some(event) = client.next().await {
        let _ = event.unwrap();
    }
}

fn duration_from_millis_str(
    s: &str,
) -> Result<std::time::Duration, <u64 as std::str::FromStr>::Err> {
    Ok(std::time::Duration::from_millis(s.parse()?))
}
