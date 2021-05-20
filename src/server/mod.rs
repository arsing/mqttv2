use futures_sink::Sink;
use futures_util::{SinkExt, StreamExt, TryStreamExt};

pub struct Server {
    listener: tokio::net::TcpListener,
}

impl Server {
    pub async fn bind(addr: impl tokio::net::ToSocketAddrs) -> std::io::Result<Self> {
        let listener = tokio::net::TcpListener::bind(addr).await?;
        Ok(Server {
            listener,
        })
    }

    pub async fn run(self) -> std::io::Result<()> {
        log::info!("Starting server...");

        let Server {
            listener,
        } = self;

        let mut next_server_generated_session_id: u64 = 0;

        let mut clients: std::collections::BTreeMap<crate::proto::ByteStr, ClientState> = Default::default();

        let mut subscriptions_by_client_id: std::collections::BTreeMap<crate::proto::ByteStr, std::collections::BTreeSet<crate::proto::ByteStr>> = Default::default();
        let mut subscriptions_by_topic: std::collections::BTreeMap<crate::proto::ByteStr, std::collections::BTreeSet<crate::proto::ByteStr>> = Default::default();

        let mut events = futures_util::stream::FuturesUnordered::new();

        events.push(RouterFuture::Accepting { listener: &listener });

        while let Some(item) = events.next().await {
            match item {
                Ok(RouterEvent::AcceptedClient { new_client }) => {
                    events.push(RouterFuture::connecting_client(new_client));
                    events.push(RouterFuture::Accepting { listener: &listener });
                },

                Ok(RouterEvent::ClientReady { new_client_id, new_client }) => {
                    let client_id = match new_client_id {
                        crate::proto::ClientId::ServerGenerated => {
                            let client_id = format!("server-generated-{}", next_server_generated_session_id).into();
                            next_server_generated_session_id += 1;
                            client_id
                        },
                        crate::proto::ClientId::IdWithCleanSession(client_id) => client_id,
                        crate::proto::ClientId::IdWithExistingSession(client_id) => client_id,
                    };

                    let (client_sink, client_stream) = new_client.split();

                    // Force inner bytes::Bytes to upgrade to Arc representation
                    let client_id = client_id.clone();

                    clients.insert(client_id.clone(), ClientState {
                        client_id: client_id.clone(),
                        pending_packets: Default::default(),
                        client_sink_and_pending_packets: Some((client_sink, Default::default())),
                    });

                    events.push(RouterFuture::ReadClient {
                        inner: Some((client_id.clone(), client_stream)),
                    });
                },

                Ok(RouterEvent::ReadClient { client_id, client_stream, packet }) => {
                    let mut response_packets: std::collections::BTreeMap<crate::proto::ByteStr, Vec<crate::proto::Packet>> = Default::default();

                    if let Some(client) = clients.get_mut(&client_id) {
                        match packet {
                            crate::proto::Packet::PingReq(crate::proto::PingReq) =>
                                if let Some(client) = clients.get_mut(&client_id) {
                                    client.write(&mut events, crate::proto::Packet::PingResp(crate::proto::PingResp));
                                },

                            crate::proto::Packet::Publish(crate::proto::Publish {
                                packet_identifier_dup_qos,
                                retain: _,
                                topic_name,
                                payload,
                            }) => {
                                match packet_identifier_dup_qos {
                                    crate::proto::PacketIdentifierDupQoS::AtMostOnce => (),
                                    crate::proto::PacketIdentifierDupQoS::AtLeastOnce(packet_identifier, _dup) => {
                                        client.write(&mut events, crate::proto::Packet::PubAck(crate::proto::PubAck {
                                            packet_identifier,
                                        }));
                                    },
                                    crate::proto::PacketIdentifierDupQoS::ExactlyOnce(_packet_identifier, _dup) => (),
                                }

                                if let Some(client_ids) = subscriptions_by_topic.get(&topic_name) {
                                    for client_id in client_ids {
                                        response_packets.entry(client_id.clone()).or_default().push(crate::proto::Packet::Publish(crate::proto::Publish {
                                            packet_identifier_dup_qos,
                                            retain: false,
                                            topic_name: topic_name.clone(),
                                            payload: payload.clone(),
                                        }));
                                    }
                                }
                            },

                            crate::proto::Packet::Subscribe(crate::proto::Subscribe {
                                packet_identifier,
                                subscribe_to,
                            }) => {
                                let mut sub_ack = crate::proto::SubAck {
                                    packet_identifier,
                                    qos: vec![],
                                };
                                for crate::proto::SubscribeTo { topic_filter, qos } in subscribe_to {
                                    let qos = match qos {
                                        crate::proto::QoS::AtMostOnce | crate::proto::QoS::AtLeastOnce => qos,
                                        crate::proto::QoS::ExactlyOnce => crate::proto::QoS::AtLeastOnce,
                                    };
                                    subscriptions_by_client_id.entry(client_id.clone()).or_default().insert(topic_filter.clone());
                                    subscriptions_by_topic.entry(topic_filter).or_default().insert(client_id.clone());
                                    sub_ack.qos.push(crate::proto::SubAckQos::Success(qos));
                                }
                                client.write(&mut events, crate::proto::Packet::SubAck(sub_ack));
                            },

                            _ => (),
                        }

                        events.push(RouterFuture::ReadClient {
                            inner: Some((client_id, client_stream)),
                        });

                        for (client_id, packets) in response_packets {
                            if let Some(client) = clients.get_mut(&client_id) {
                                for packet in packets {
                                    client.write(&mut events, packet);
                                }
                            }
                        }
                    }
                },

                Ok(RouterEvent::WriteClient { client_id, client_sink, mut pending_packets }) =>
                    if let Some(client) = clients.get_mut(&client_id) {
                        if client.pending_packets.is_empty() {
                            client.client_sink_and_pending_packets = Some((client_sink, pending_packets))
                        }
                        else {
                            pending_packets.extend(client.pending_packets.drain(..));
                            events.push(RouterFuture::WriteClient {
                                inner: Some((client_id, client_sink, pending_packets)),
                            });
                        }
                    },

                Err(err) => {
                    log::info!("dropping client because of error: {}", err);
                    // TODO: remove from state, etc
                },
            }
        }

        Ok(())
    }
}

fn auth(connect: &crate::proto::Connect) -> Result<(), crate::proto::ConnectionRefusedReason> {
    log::info!("authorizing {:?}:{:?}:{:?}", connect.username, connect.password, connect.client_id);
    Ok(())
}

#[derive(Debug)]
struct ClientState {
    client_id: crate::proto::ByteStr,
    pending_packets: std::collections::VecDeque<crate::proto::Packet>,
    client_sink_and_pending_packets: Option<(ClientSink, std::collections::VecDeque<crate::proto::Packet>)>,
}

impl ClientState {
    fn write<F>(&mut self, events: &mut futures_util::stream::FuturesUnordered<RouterFuture<'_, F>>, packet: crate::proto::Packet) {
        if let Some((client_sink, mut pending_packets)) = self.client_sink_and_pending_packets.take() {
            pending_packets.extend(self.pending_packets.drain(..));
            pending_packets.push_back(packet);
            events.push(RouterFuture::WriteClient {
                inner: Some((self.client_id.clone(), client_sink, pending_packets)),
            });
        }
        else {
            self.pending_packets.push_back(packet);
        }
    }
}

type Client = crate::logging_framed::LoggingFramed<tokio::net::TcpStream>;
type ClientStream = futures_util::stream::SplitStream<Client>;
type ClientSink = futures_util::stream::SplitSink<Client, crate::proto::Packet>;

#[pin_project::pin_project(project = RouterFutureProj)]
enum RouterFuture<'a, TConnectingClientFuture> {
    Accepting {
        listener: &'a tokio::net::TcpListener,
    },

    ConnectingClient {
        #[pin] inner: TConnectingClientFuture,
    },

    ReadClient {
        inner: Option<(crate::proto::ByteStr, ClientStream)>,
    },

    WriteClient {
        inner: Option<(crate::proto::ByteStr, ClientSink, std::collections::VecDeque<crate::proto::Packet>)>,
    },
}

impl RouterFuture<'static, ()> {
    fn connecting_client(mut framed: Client) -> RouterFuture<'static, impl std::future::Future<Output = Result<(crate::proto::ClientId, Client), ServerError>>> {
        RouterFuture::ConnectingClient {
            inner: async move {
                let packet = framed.try_next().await?.ok_or(ServerError::ClientUnexpectedEof)?;
                let connect =
                    if let crate::proto::Packet::Connect(connect) = packet {
                        connect
                    }
                    else {
                        return Err(ServerError::ClientUnexpected { expected: "CONNECT" });
                    };
                if let Err(reason) = auth(&connect) {
                    framed.send(crate::proto::Packet::ConnAck(crate::proto::ConnAck {
                        session_present: false,
                        return_code: crate::proto::ConnectReturnCode::Refused(reason),
                    })).await?;
                    return Err(ServerError::ClientAuthFailed);
                }

                framed.send(crate::proto::Packet::ConnAck(crate::proto::ConnAck {
                    session_present: false,
                    return_code: crate::proto::ConnectReturnCode::Accepted,
                })).await?;

                Ok((connect.client_id, framed))
            },
        }
    }
}

impl<TConnectingClientFuture> std::future::Future for RouterFuture<'_, TConnectingClientFuture>
where
    TConnectingClientFuture: std::future::Future<Output = Result<(crate::proto::ClientId, Client), ServerError>>,
{
    type Output = Result<RouterEvent, ServerError>;

    fn poll(mut self: std::pin::Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> std::task::Poll<Self::Output> {
        std::task::Poll::Ready(Ok(match self.as_mut().project() {
            RouterFutureProj::Accepting { listener } => match listener.poll_accept(cx) {
                std::task::Poll::Ready(stream) => {
                    let (stream, _) = stream.expect("couldn't accept client");
                    RouterEvent::AcceptedClient {
                        new_client: crate::logging_framed::LoggingFramed::new(stream),
                    }
                },
                std::task::Poll::Pending => return std::task::Poll::Pending,
            },
 
            RouterFutureProj::ConnectingClient { inner } => match inner.poll(cx)? {
                std::task::Poll::Ready((new_client_id, new_client)) => RouterEvent::ClientReady {
                    new_client_id,
                    new_client,
                },
                std::task::Poll::Pending => return std::task::Poll::Pending,
            },

            RouterFutureProj::ReadClient { inner } => {
                let (client_id, mut client_stream) = inner.take().expect("polled after completion");
                match client_stream.try_poll_next_unpin(cx)? {
                    std::task::Poll::Ready(packet) => {
                        let packet = packet.ok_or(ServerError::ClientUnexpectedEof)?;
                        RouterEvent::ReadClient {
                            client_id,
                            client_stream,
                            packet,
                        }
                    },
                    std::task::Poll::Pending => {
                        self.set(RouterFuture::ReadClient {
                            inner: Some((client_id, client_stream)),
                        });
                        return std::task::Poll::Pending;
                    },
                }
            },

            RouterFutureProj::WriteClient { inner } => {
                let (client_id, mut client_sink, mut pending_packets) = inner.take().expect("polled after completion");

                while let Some(packet) = pending_packets.pop_front() {
                    match std::pin::Pin::new(&mut client_sink).poll_ready(cx)? {
                        std::task::Poll::Ready(()) => {
                            let () = std::pin::Pin::new(&mut client_sink).start_send(packet)?;
                        },
                        std::task::Poll::Pending => {
                            pending_packets.push_front(packet);
                            self.set(RouterFuture::WriteClient {
                                inner: Some((client_id, client_sink, pending_packets)),
                            });
                            return std::task::Poll::Pending;
                        },
                    }
                }

                match std::pin::Pin::new(&mut client_sink).poll_flush(cx)? {
                    std::task::Poll::Ready(()) => RouterEvent::WriteClient {
                        client_id,
                        client_sink,
                        pending_packets,
                    },
                    std::task::Poll::Pending => {
                        self.set(RouterFuture::WriteClient {
                            inner: Some((client_id, client_sink, pending_packets)),
                        });
                        return std::task::Poll::Pending;
                    },
                }

            },
        }))
    }
}

enum RouterEvent {
    AcceptedClient {
        new_client: Client,
    },

    ClientReady {
        new_client_id: crate::proto::ClientId,
        new_client: Client,
    },

    ReadClient {
        client_id: crate::proto::ByteStr,
        client_stream: ClientStream,
        packet: crate::proto::Packet,
    },

    WriteClient {
        client_id: crate::proto::ByteStr,
        client_sink: ClientSink,
        pending_packets: std::collections::VecDeque<crate::proto::Packet>,
    },
}

#[derive(Debug)]
enum ServerError {
    ClientAuthFailed,
    ClientMalformed(crate::proto::DecodeError),
    ClientUnexpected { expected: &'static str },
    ClientUnexpectedEof,
    ServerMalformed(crate::proto::EncodeError),
}

impl std::fmt::Display for ServerError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ServerError::ClientAuthFailed => f.write_str("client auth failed"),
            ServerError::ClientMalformed(_) => f.write_str("client sent malformed packet"),
            ServerError::ClientUnexpected { expected } => write!(f, "client sent unexpected packet, expected {:?}", expected),
            ServerError::ClientUnexpectedEof => f.write_str("client disconnected unexpectedly"),
            ServerError::ServerMalformed(_) => f.write_str("server sent malformed packet"),
        }
    }
}

impl std::error::Error for ServerError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        #[allow(clippy::match_same_arms)]
        match self {
            ServerError::ClientAuthFailed => None,
            ServerError::ClientMalformed(err) => Some(err),
            ServerError::ClientUnexpected { .. } => None,
            ServerError::ClientUnexpectedEof => None,
            ServerError::ServerMalformed(err) => Some(err),
        }
    }
}

impl From<crate::proto::DecodeError> for ServerError {
    fn from(err: crate::proto::DecodeError) -> Self {
        ServerError::ClientMalformed(err)
    }
}

impl From<crate::proto::EncodeError> for ServerError {
    fn from(err: crate::proto::EncodeError) -> Self {
        ServerError::ServerMalformed(err)
    }
}
