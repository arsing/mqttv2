use std::convert::TryInto;

use futures_sink::Sink;
use futures_util::{FutureExt, SinkExt, StreamExt, TryStreamExt};

type AuthAcceptedClientFuture<L> = std::pin::Pin<Box<dyn std::future::Future<Output = Result<
    (crate::proto::ClientId, <L as crate::io::Listener>::PacketStream, <L as crate::io::Listener>::PacketSink),
    ServerError,
>>>>;

pub fn run<L>(listener: L) -> impl std::future::Future<Output = std::io::Result<()>>
where
    L: crate::io::Listener + Unpin,
    <L as crate::io::Listener>::PacketStream: Unpin + 'static,
    <L as crate::io::Listener>::PacketSink: Unpin + 'static,
{
    struct Run<L> where L: crate::io::Listener {
        server_state: ServerState<L>,
        events_accept: futures_util::stream::FuturesUnordered<RouterFutureAccept<L>>,
        events_recv: futures_util::stream::FuturesUnordered<RouterFutureRecv<L>>,
        events_send: futures_util::stream::FuturesUnordered<RouterFutureSend<L>>,
    }

    impl<L> std::future::Future for Run<L>
    where
        L: crate::io::Listener + Unpin,
        <L as crate::io::Listener>::PacketStream: Unpin + 'static,
        <L as crate::io::Listener>::PacketSink: Unpin + 'static,
    {
        type Output = std::io::Result<()>;

        fn poll(mut self: std::pin::Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> std::task::Poll<Self::Output> {
            let this = &mut *self;

            loop {
                let mut all_pending = true;

                // Write as much as possible, then read once, then accept once.

                while let std::task::Poll::Ready(Some(RouterEventSend { client_id, result })) = this.events_send.poll_next_unpin(cx) {
                    log::trace!("RouterEventSend({:?})", client_id);
                    all_pending = false;

                    match result {
                        Ok((client_sink, mut pending_packets)) =>
                            if let Some(client) = this.server_state.get_client_mut(&client_id) {
                                if client.pending_packets.is_empty() {
                                    client.client_sink_and_pending_packets = Some((client_sink, pending_packets))
                                }
                                else {
                                    std::mem::swap(&mut pending_packets, &mut client.pending_packets);
                                    this.events_send.push(RouterFutureSend(Some((client_id, client_sink, pending_packets))));
                                }
                            },

                        Err(err) => {
                            log::info!("dropping unwritable client {} because of error: {}", client_id, err);
                            this.server_state.drop_client(&client_id);
                        },
                    }
                }

                if let std::task::Poll::Ready(Some(item)) = this.events_accept.poll_next_unpin(cx) {
                    log::trace!("RouterEventAccept");
                    all_pending = false;

                    match item {
                        RouterEventAccept::AcceptedClient(listener, Ok((new_client_stream, new_client_sink))) => {
                            this.events_accept.push(auth_accepted_client(new_client_stream, new_client_sink));
                            this.events_accept.push(RouterFutureAccept::Accepting { listener: Some(listener) });
                        },

                        RouterEventAccept::AcceptedClient(listener, Err(err)) => {
                            log::info!("dropping pre-accepted client because of error: {}", err);
                            this.events_accept.push(RouterFutureAccept::Accepting { listener: Some(listener) });
                        },

                        RouterEventAccept::ClientReady(Ok((new_client_id, new_client_stream, new_client_sink))) => {
                            let client_id = this.server_state.add_client(new_client_id, new_client_sink);

                            this.events_recv.push(RouterFutureRecv(Some((client_id.clone(), new_client_stream))));
                        },

                        RouterEventAccept::ClientReady(Err(err)) => {
                            log::info!("dropping post-accepted client because of error: {}", err);
                        },
                    }
                }

                if let std::task::Poll::Ready(Some(RouterEventRecv { client_id, result })) = this.events_recv.poll_next_unpin(cx) {
                    all_pending = false;

                    match result {
                        Ok((client_stream, packet)) => {
                            #[allow(clippy::mutable_key_type)]
                            let mut response_packets: std::collections::BTreeMap<crate::proto::ByteStr, Vec<crate::proto::Packet>> = Default::default();

                            if let Some(client) = this.server_state.get_client_mut(&client_id) {
                                match packet {
                                    crate::proto::Packet::PingReq(crate::proto::PingReq) =>
                                        client.write(&mut this.events_send, crate::proto::Packet::PingResp(crate::proto::PingResp)),

                                    crate::proto::Packet::Publish(crate::proto::Publish {
                                        packet_identifier_dup_qos,
                                        retain: _,
                                        topic_name,
                                        payload,
                                    }) => {
                                        match packet_identifier_dup_qos {
                                            crate::proto::PacketIdentifierDupQoS::AtMostOnce => (),
                                            crate::proto::PacketIdentifierDupQoS::AtLeastOnce(packet_identifier, _dup) => {
                                                client.write(&mut this.events_send, crate::proto::Packet::PubAck(crate::proto::PubAck {
                                                    packet_identifier,
                                                }));
                                            },
                                            crate::proto::PacketIdentifierDupQoS::ExactlyOnce(_packet_identifier, _dup) => (),
                                        }

                                        if let Some(client_ids) = this.server_state.get_subscribers(&topic_name) {
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
                                            this.server_state.subscribe(client_id.clone(), topic_filter.clone());
                                            sub_ack.qos.push(crate::proto::SubAckQos::Success(qos));
                                        }
                                        let client = this.server_state.get_client_mut(&client_id).expect("got this client successfully just before this");
                                        client.write(&mut this.events_send, crate::proto::Packet::SubAck(sub_ack));
                                    },

                                    _ => (),
                                }
                            }

                            this.events_recv.push(RouterFutureRecv(Some((client_id, client_stream))));

                            for (client_id, packets) in response_packets {
                                if let Some(client) = this.server_state.get_client_mut(&client_id) {
                                    for packet in packets {
                                        client.write(&mut this.events_send, packet);
                                    }
                                }
                            }
                        },

                        Err(err) => {
                            log::info!("dropping unreadable client {} because of error: {}", client_id, err);
                            this.server_state.drop_client(&client_id);
                        },
                    }
                }

                if all_pending {
                    return std::task::Poll::Pending;
                }
            }
        }
    }

    log::info!("Starting server...");

    Run {
        server_state: Default::default(),
        events_accept: std::iter::once(RouterFutureAccept::Accepting { listener: Some(listener) }).collect(),
        events_recv: Default::default(),
        events_send: Default::default(),
    }
}

#[allow(clippy::unnecessary_wraps)]
fn auth(connect: &crate::proto::Connect) -> Result<(), crate::proto::ConnectionRefusedReason> {
    log::info!("authorizing {:?}:{:?}:{:?}", connect.username, connect.password, connect.client_id);
    Ok(())
}

struct ServerState<L> where L: crate::io::Listener {
    next_server_generated_session_id: u64,

    #[allow(clippy::mutable_key_type)]
    clients: std::collections::BTreeMap<crate::proto::ByteStr, ClientState<L>>,

    #[allow(clippy::mutable_key_type)]
    subscriptions_by_client_id: std::collections::BTreeMap<crate::proto::ByteStr, std::collections::BTreeSet<crate::proto::ByteStr>>,

    #[allow(clippy::mutable_key_type)]
    subscriptions_by_topic: std::collections::BTreeMap<crate::proto::ByteStr, std::collections::BTreeSet<crate::proto::ByteStr>>,
}

impl<L> ServerState<L> where L: crate::io::Listener {
    fn add_client(
        &mut self,
        client_id: crate::proto::ClientId,
        client_sink: <L as crate::io::Listener>::PacketSink,
    ) -> crate::proto::ByteStr {
        let client_id = match client_id {
            crate::proto::ClientId::ServerGenerated => {
                let client_id =
                    format!("server-generated-{}", self.next_server_generated_session_id)
                    .try_into().expect("this string will not be long enough to exceed u16::max_value() bytes");
                self.next_server_generated_session_id += 1;
                client_id
            },
            crate::proto::ClientId::IdWithCleanSession(client_id) |
            crate::proto::ClientId::IdWithExistingSession(client_id) => client_id,
        };

        self.clients.insert(client_id.clone(), ClientState {
            client_id: client_id.clone(),
            pending_packets: Default::default(),
            client_sink_and_pending_packets: Some((client_sink, Default::default())),
        });

        client_id
    }

    fn get_client_mut(&mut self, client_id: &crate::proto::ByteStr) -> Option<&mut ClientState<L>> {
        self.clients.get_mut(&client_id)
    }

    fn drop_client(&mut self, client_id: &crate::proto::ByteStr) {
        self.clients.remove(&client_id);
        if let Some(subscriptions) = self.subscriptions_by_client_id.remove(&client_id) {
            for topic in subscriptions {
                if let Some(client_ids) = self.subscriptions_by_topic.get_mut(&topic) {
                    client_ids.remove(&client_id);
                }
            }
        }
    }

    fn subscribe(&mut self, client_id: crate::proto::ByteStr, topic_filter: crate::proto::ByteStr) {
        self.subscriptions_by_client_id.entry(client_id.clone()).or_default().insert(topic_filter.clone());
        self.subscriptions_by_topic.entry(topic_filter).or_default().insert(client_id);
    }

    fn get_subscribers(&self, topic_name: &crate::proto::ByteStr) -> Option<&std::collections::BTreeSet<crate::proto::ByteStr>> {
        // TODO: wildcards
        self.subscriptions_by_topic.get(topic_name)
    }
}

impl<L> Default for ServerState<L> where L: crate::io::Listener {
    fn default() -> Self {
        ServerState {
            next_server_generated_session_id: Default::default(),
            clients: Default::default(),
            subscriptions_by_client_id: Default::default(),
            subscriptions_by_topic: Default::default(),
        }
    }
}

struct ClientState<L> where L: crate::io::Listener {
    client_id: crate::proto::ByteStr,
    pending_packets: std::collections::VecDeque<crate::proto::Packet>,
    client_sink_and_pending_packets: Option<(<L as crate::io::Listener>::PacketSink, std::collections::VecDeque<crate::proto::Packet>)>,
}

impl<L> ClientState<L> where L: crate::io::Listener {
    fn write(&mut self, events: &mut futures_util::stream::FuturesUnordered<RouterFutureSend<L>>, packet: crate::proto::Packet) {
        if let Some((client_sink, mut pending_packets)) = self.client_sink_and_pending_packets.take() {
            pending_packets.extend(self.pending_packets.drain(..));
            pending_packets.push_back(packet);
            events.push(RouterFutureSend(Some((self.client_id.clone(), client_sink, pending_packets))));
        }
        else {
            self.pending_packets.push_back(packet);
        }
    }
}

enum RouterFutureAccept<L> where L: crate::io::Listener {
    Accepting {
        listener: Option<L>,
    },

    ConnectingClient {
        inner: AuthAcceptedClientFuture<L>,
    },
}

fn auth_accepted_client<L>(
    mut stream: <L as crate::io::Listener>::PacketStream,
    mut sink: <L as crate::io::Listener>::PacketSink,
) -> RouterFutureAccept<L>
where
    L: crate::io::Listener + Unpin,
    <L as crate::io::Listener>::PacketStream: Unpin + 'static,
    <L as crate::io::Listener>::PacketSink: Unpin + 'static,
{
    RouterFutureAccept::ConnectingClient {
        inner: Box::pin(async move {
            let packet = stream.try_next().await?.ok_or(ServerError::ClientUnexpectedEof)?;
            let connect =
                if let crate::proto::Packet::Connect(connect) = packet {
                    connect
                }
                else {
                    return Err(ServerError::ClientUnexpected { expected: "CONNECT" });
                };
            if let Err(reason) = auth(&connect) {
                sink.send(crate::proto::Packet::ConnAck(crate::proto::ConnAck {
                    session_present: false,
                    return_code: crate::proto::ConnectReturnCode::Refused(reason),
                })).await?;
                return Err(ServerError::ClientAuthFailed);
            }

            sink.send(crate::proto::Packet::ConnAck(crate::proto::ConnAck {
                session_present: false,
                return_code: crate::proto::ConnectReturnCode::Accepted,
            })).await?;

            Ok((connect.client_id, stream, sink))
        }),
    }
}

impl<L> std::future::Future for RouterFutureAccept<L>
where
    L: crate::io::Listener + Unpin,
    <L as crate::io::Listener>::PacketStream: Unpin,
    <L as crate::io::Listener>::PacketSink: Unpin,
{
    type Output = RouterEventAccept<L>;

    fn poll(mut self: std::pin::Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> std::task::Poll<Self::Output> {
        std::task::Poll::Ready(match &mut *self {
            RouterFutureAccept::Accepting { listener } => {
                let mut listener = listener.take().expect("polled after completion");
                match listener.poll_accept(cx) {
                    std::task::Poll::Ready(new_client) => RouterEventAccept::AcceptedClient(listener, new_client.map_err(ServerError::ClientAcceptFailed)),
                    std::task::Poll::Pending => {
                        self.set(RouterFutureAccept::Accepting {
                            listener: Some(listener),
                        });
                        return std::task::Poll::Pending;
                    },
                }
            },

            RouterFutureAccept::ConnectingClient { inner } => match inner.poll_unpin(cx) {
                std::task::Poll::Ready(result) => RouterEventAccept::ClientReady(result),
                std::task::Poll::Pending => return std::task::Poll::Pending,
            },
        })
    }
}

enum RouterEventAccept<L> where L: crate::io::Listener {
    AcceptedClient(L, Result<(<L as crate::io::Listener>::PacketStream, <L as crate::io::Listener>::PacketSink), ServerError>),

    ClientReady(Result<(crate::proto::ClientId, <L as crate::io::Listener>::PacketStream, <L as crate::io::Listener>::PacketSink), ServerError>),
}

struct RouterFutureRecv<L>(Option<(crate::proto::ByteStr, <L as crate::io::Listener>::PacketStream)>) where L: crate::io::Listener;

impl<L> std::future::Future for RouterFutureRecv<L>
where
    L: crate::io::Listener + Unpin,
    <L as crate::io::Listener>::PacketStream: Unpin,
    <L as crate::io::Listener>::PacketSink: Unpin,
{
    type Output = RouterEventRecv<L>;

    fn poll(mut self: std::pin::Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> std::task::Poll<Self::Output> {
        let (client_id, mut client_stream) = self.0.take().expect("polled after completion");
        match client_stream.try_poll_next_unpin(cx) {
            std::task::Poll::Ready(packet) => {
                let result = match packet {
                    Some(Ok(packet)) => Ok((client_stream, packet)),
                    Some(Err(err)) => Err(err.into()),
                    None => Err(ServerError::ClientUnexpectedEof),
                };
                std::task::Poll::Ready(RouterEventRecv {
                    client_id,
                    result,
                })
            },
            std::task::Poll::Pending => {
                self.set(RouterFutureRecv(Some((client_id, client_stream))));
                std::task::Poll::Pending
            },
        }
    }
}

struct RouterEventRecv<L> where L: crate::io::Listener {
    client_id: crate::proto::ByteStr,
    result: Result<(<L as crate::io::Listener>::PacketStream, crate::proto::Packet), ServerError>,
}

struct RouterFutureSend<L>(Option<(crate::proto::ByteStr, <L as crate::io::Listener>::PacketSink, std::collections::VecDeque<crate::proto::Packet>)>) where L: crate::io::Listener;

impl<L> std::future::Future for RouterFutureSend<L>
where
    L: crate::io::Listener + Unpin,
    <L as crate::io::Listener>::PacketStream: Unpin,
    <L as crate::io::Listener>::PacketSink: Unpin,
{
    type Output = RouterEventSend<L>;

    fn poll(mut self: std::pin::Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> std::task::Poll<Self::Output> {
        log::trace!("RouterFutureSend polled");
        let (client_id, mut client_sink, mut pending_packets) = self.0.take().expect("polled after completion");

        while let Some(packet) = pending_packets.pop_front() {
            match std::pin::Pin::new(&mut client_sink).poll_ready(cx) {
                std::task::Poll::Ready(Ok(())) =>
                    match std::pin::Pin::new(&mut client_sink).start_send(packet) {
                        Ok(()) => (),
                        Err(err) => return std::task::Poll::Ready(RouterEventSend {
                            client_id,
                            result: Err(err.into()),
                        }),
                    },

                std::task::Poll::Ready(Err(err)) =>
                    return std::task::Poll::Ready(RouterEventSend {
                        client_id,
                        result: Err(err.into()),
                    }),

                std::task::Poll::Pending => {
                    pending_packets.push_front(packet);
                    self.set(RouterFutureSend(Some((client_id, client_sink, pending_packets))));
                    return std::task::Poll::Pending;
                },
            }
        }

        std::task::Poll::Ready(match std::pin::Pin::new(&mut client_sink).poll_flush(cx) {
            std::task::Poll::Ready(Ok(())) => RouterEventSend {
                client_id,
                result: Ok((client_sink, pending_packets)),
            },

            std::task::Poll::Ready(Err(err)) => RouterEventSend {
                client_id,
                result: Err(err.into()),
            },

            std::task::Poll::Pending => {
                self.set(RouterFutureSend(Some((client_id, client_sink, pending_packets))));
                return std::task::Poll::Pending;
            },
        })
    }
}

struct RouterEventSend<L> where L: crate::io::Listener {
    client_id: crate::proto::ByteStr,
    result: Result<(<L as crate::io::Listener>::PacketSink, std::collections::VecDeque<crate::proto::Packet>), ServerError>,
}

#[derive(Debug)]
enum ServerError {
    ClientAcceptFailed(std::io::Error),
    ClientAuthFailed,
    ClientMalformed(crate::proto::DecodeError),
    ClientUnexpected { expected: &'static str },
    ClientUnexpectedEof,
    ServerMalformed(crate::proto::EncodeError),
}

impl std::fmt::Display for ServerError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ServerError::ClientAcceptFailed(_) => f.write_str("could not accept client"),
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
            ServerError::ClientAcceptFailed(err) => Some(err),
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
