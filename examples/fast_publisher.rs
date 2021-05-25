// Example:
//
//     cargo run --release --example fast_publisher -- --payload-len 32

mod common;

use std::convert::TryInto;
use std::io::{Read, Write};

#[derive(Debug, structopt::StructOpt)]
struct Options {
    /// Address of the MQTT server.
    #[structopt(long, default_value = "[::1]:1883")]
    server: std::net::SocketAddr,

    /// The length of the payload.
    #[structopt(long)]
    payload_len: usize,
}

fn main() {
    let Options {
        server,
        payload_len,
    } = common::init("fast_publisher");

    let payload: bytes::Bytes = std::iter::repeat("1234567890").flat_map(str::as_bytes).take(payload_len).copied().collect();
    let payload = std::convert::TryInto::try_into(payload).unwrap();

    let mut stream = std::net::TcpStream::connect(server).unwrap();

    let connect = encode(mqtt3::proto::Packet::Connect(mqtt3::proto::Connect {
        username: None,
        password: None,
        will: None,
        client_id: mqtt3::proto::ClientId::ServerGenerated,
        keep_alive: std::time::Duration::from_secs(0),
        protocol_name: mqtt3::PROTOCOL_NAME,
        protocol_level: mqtt3::PROTOCOL_LEVEL,
    }));
    let () = stream.write_all(&*connect).unwrap();

    let mut conn_ack = [0_u8; 4];
    let () = stream.read_exact(&mut conn_ack).unwrap();
    assert_eq!(conn_ack, *b"\x20\x02\x00\x00");

    let publish = encode(mqtt3::proto::Packet::Publish(mqtt3::proto::Publish {
        packet_identifier_dup_qos: mqtt3::proto::PacketIdentifierDupQoS::AtMostOnce,
        retain: false,
        topic_name: "foo".to_owned().try_into().unwrap(),
        payload,
    }));
    let mut publish_buf = Vec::with_capacity(8192);

    let mut num_publish_packets_per_write = 0;
    loop {
        publish_buf.extend_from_slice(&publish);
        num_publish_packets_per_write += 1;
        if publish_buf.len() + publish.len() > 8192 {
            break;
        }
    }

    let publish = &*publish_buf;
    log::info!("Writing {} packets ({} bytes) per write", num_publish_packets_per_write, publish.len());

    let mut packet_stats: common::PacketStats = Default::default();

    loop {
        let _ = stream.write_all(publish).unwrap();

        packet_stats.count(num_publish_packets_per_write);
    }
}

fn encode(packet: mqtt3::proto::Packet) -> bytes::BytesMut {
    let mut buf = bytes::BytesMut::new();
    mqtt3::proto::encode(packet, &mut buf).unwrap();
    buf
}
