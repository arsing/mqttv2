use std::io::{Read, Write};

fn main() {
    let mut stream = std::net::TcpStream::connect(("127.0.0.1", 1883)).unwrap();

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
    assert_eq!(conn_ack, b"\x20\x02\x00\x00"[..]);

    let publish = encode(mqtt3::proto::Packet::Publish(mqtt3::proto::Publish {
        packet_identifier_dup_qos: mqtt3::proto::PacketIdentifierDupQoS::AtMostOnce,
        retain: false,
        topic_name: "foo".to_owned().into(),
        payload: "12345678901234567890123456789012".into(),
    }));
    let mut publish_buf = Vec::with_capacity(8192);
    let mut num_publish_packets_per_write = 0;
    while publish_buf.len() + publish.len() < 8192 {
        publish_buf.extend_from_slice(&publish);
        num_publish_packets_per_write += 1;
    }
    let publish = &*publish_buf;
    eprintln!("Writing {} packets ({} bytes) per write", num_publish_packets_per_write, publish.len());

    let mut stats_num_packets = 0;
    let mut stats_start_time = std::time::Instant::now();

    loop {
        let _ = stream.write_all(publish).unwrap();

        stats_num_packets += num_publish_packets_per_write;

        let now = std::time::Instant::now();
        let elapsed = now.duration_since(stats_start_time);
        if elapsed > std::time::Duration::from_secs(1) {
            eprintln!("packets: {} ({}/sec)", stats_num_packets, stats_num_packets * 1_000_000 / elapsed.as_micros());
            stats_start_time = now;
            stats_num_packets = 0;
        }
    }
}

fn encode(packet: mqtt3::proto::Packet) -> bytes::BytesMut {
    let mut buf = bytes::BytesMut::new();
    mqtt3::proto::encode(packet, &mut buf).unwrap();
    buf
}
