// Example:
//
//     cargo run --release --example ping -- --payload-len 32

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
    } = common::init("ping");

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

    let subscribe = encode(mqtt3::proto::Packet::Subscribe(mqtt3::proto::Subscribe {
        packet_identifier: mqtt3::proto::PacketIdentifier::new(1).unwrap(),
        subscribe_to: vec![mqtt3::proto::SubscribeTo {
            topic_filter: "foo".to_owned().try_into().unwrap(),
            qos: mqtt3::proto::QoS::AtMostOnce,
        }],
    }));
    let () = stream.write_all(&*subscribe).unwrap();
    let mut sub_ack = [0_u8; 5];
    let () = stream.read_exact(&mut sub_ack).unwrap();
    assert_eq!(sub_ack, *b"\x90\x03\x00\x01\x00");

    let publish = encode(mqtt3::proto::Packet::Publish(mqtt3::proto::Publish {
        packet_identifier_dup_qos: mqtt3::proto::PacketIdentifierDupQoS::AtMostOnce,
        retain: false,
        topic_name: "foo".to_owned().try_into().unwrap(),
        payload,
    }));
    let publish = &*publish;

    let mut recv = vec![0_u8; publish.len()];
    loop {
        let start_send_time = std::time::Instant::now();
        let () = stream.write_all(publish).unwrap();
        let finish_send_time = std::time::Instant::now();

        let () = stream.read_exact(&mut recv).unwrap();
        let finish_recv_time = std::time::Instant::now();
        assert_eq!(recv, *publish);
        log::info!(
            "start_send = {:?}, finish_send = {:?} (+{}ms), finish_recv = {:?} (+{}ms)",
            start_send_time,
            finish_send_time, (finish_send_time - start_send_time).as_millis(),
            finish_recv_time, (finish_recv_time - finish_send_time).as_millis(),
        );
    }
}

fn encode(packet: mqtt3::proto::Packet) -> bytes::BytesMut {
    let mut buf = bytes::BytesMut::new();
    mqtt3::proto::encode(packet, &mut buf).unwrap();
    buf
}
