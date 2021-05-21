use std::future::Future;

#[derive(Debug)]
pub(super) struct Connect<C>
where
    C: crate::io::Connector,
{
    connector: C,
    max_back_off: std::time::Duration,
    current_back_off: std::time::Duration,
    state: State<C>,
}

enum State<C>
where
    C: crate::io::Connector,
{
    BeginBackOff,
    EndBackOff(std::pin::Pin<Box<tokio::time::Sleep>>),
    BeginConnecting,
    WaitingForIoToConnect(<C as crate::io::Connector>::Future),
    Framed {
        stream: <C as crate::io::Connector>::PacketStream,
        sink: <C as crate::io::Connector>::PacketSink,
        framed_state: FramedState,
        password: Option<crate::proto::ByteStr>,
    },
}

#[derive(Clone, Copy, Debug)]
enum FramedState {
    BeginSendingConnect,
    EndSendingConnect,
    WaitingForConnAck,
    Connected {
        new_connection: bool,
        reset_session: bool,
    },
}

impl<C> std::fmt::Debug for State<C>
where
    C: crate::io::Connector,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            State::BeginBackOff => f.write_str("BeginBackOff"),
            State::EndBackOff(_) => f.write_str("EndBackOff"),
            State::BeginConnecting => f.write_str("BeginConnecting"),
            State::WaitingForIoToConnect(_) => f.write_str("WaitingForIoToConnect"),
            State::Framed { framed_state, .. } => f
                .debug_struct("Framed")
                .field("framed_state", framed_state)
                .finish(),
        }
    }
}

impl<C> Connect<C>
where
    C: crate::io::Connector,
{
    pub(super) fn new(connector: C, max_back_off: std::time::Duration) -> Self {
        Connect {
            connector,
            max_back_off,
            current_back_off: std::time::Duration::from_secs(0),
            state: State::BeginConnecting,
        }
    }

    pub(super) fn reconnect(&mut self) {
        self.state = State::BeginBackOff;
    }
}

impl<C> Connect<C>
where
    C: crate::io::Connector,
    <C as crate::io::Connector>::PacketStream: Unpin,
    <C as crate::io::Connector>::PacketSink: Unpin,
    <C as crate::io::Connector>::Error: std::fmt::Display,
    <C as crate::io::Connector>::Future: Unpin,
{
    pub(super) fn poll<'a>(
        &'a mut self,
        cx: &mut std::task::Context<'_>,

        username: Option<&crate::proto::ByteStr>,
        will: Option<&crate::proto::Publication>,
        client_id: &mut crate::proto::ClientId,
        keep_alive: std::time::Duration,
    ) -> std::task::Poll<Connected<'a, C>> {
        use futures_core::Stream;
        use futures_sink::Sink;

        let state = &mut self.state;

        loop {
            log::trace!("    {:?}", state);

            match state {
                State::BeginBackOff => match self.current_back_off {
                    back_off if back_off.as_secs() == 0 => {
                        self.current_back_off = std::time::Duration::from_secs(1);
                        *state = State::BeginConnecting;
                    }

                    back_off => {
                        log::debug!("Backing off for {:?}", back_off);
                        self.current_back_off =
                            std::cmp::min(self.max_back_off, self.current_back_off * 2);
                        *state = State::EndBackOff(Box::pin(tokio::time::sleep(back_off)));
                    }
                },

                State::EndBackOff(back_off_timer) => {
                    use futures_util::FutureExt;
                    match back_off_timer.poll_unpin(cx) {
                        // match std::pin::Pin::new(back_off_timer).poll(cx) {
                        std::task::Poll::Ready(()) => *state = State::BeginConnecting,
                        std::task::Poll::Pending => return std::task::Poll::Pending,
                    }
                }

                State::BeginConnecting => {
                    let io = self.connector.connect();
                    *state = State::WaitingForIoToConnect(io);
                }

                State::WaitingForIoToConnect(io) => match std::pin::Pin::new(io).poll(cx) {
                    std::task::Poll::Ready(Ok((stream, sink, password))) => {
                        *state = State::Framed {
                            stream,
                            sink,
                            framed_state: FramedState::BeginSendingConnect,
                            password,
                        };
                    }

                    std::task::Poll::Ready(Err(err)) => {
                        log::warn!("could not connect to server: {}", err);
                        *state = State::BeginBackOff;
                    }

                    std::task::Poll::Pending => return std::task::Poll::Pending,
                },

                State::Framed {
                    stream: _,
                    sink,
                    framed_state: framed_state @ FramedState::BeginSendingConnect,
                    password,
                } => match std::pin::Pin::new(&mut *sink).poll_ready(cx) {
                    std::task::Poll::Ready(Ok(())) => {
                        let packet = crate::proto::Packet::Connect(crate::proto::Connect {
                            username: username.cloned(),
                            password: password.clone(),
                            will: will.cloned(),
                            client_id: client_id.clone(),
                            keep_alive,
                            protocol_name: crate::PROTOCOL_NAME,
                            protocol_level: crate::PROTOCOL_LEVEL,
                        });

                        match std::pin::Pin::new(&mut *sink).start_send(packet) {
                            Ok(()) => *framed_state = FramedState::EndSendingConnect,
                            Err(err) => {
                                log::warn!("could not connect to server: {}", err);
                                *state = State::BeginBackOff;
                            }
                        }
                    }

                    std::task::Poll::Ready(Err(err)) => {
                        log::warn!("could not connect to server: {}", err);
                        *state = State::BeginBackOff;
                    }

                    std::task::Poll::Pending => return std::task::Poll::Pending,
                },

                State::Framed {
                    stream: _,
                    sink,
                    framed_state: framed_state @ FramedState::EndSendingConnect,
                    ..
                } => match std::pin::Pin::new(sink).poll_flush(cx) {
                    std::task::Poll::Ready(Ok(())) => {
                        *framed_state = FramedState::WaitingForConnAck
                    }
                    std::task::Poll::Ready(Err(err)) => {
                        log::warn!("could not connect to server: {}", err);
                        *state = State::BeginBackOff;
                    }
                    std::task::Poll::Pending => return std::task::Poll::Pending,
                },

                State::Framed {
                    stream,
                    sink: _,
                    framed_state: framed_state @ FramedState::WaitingForConnAck,
                    ..
                } => match std::pin::Pin::new(stream).poll_next(cx) {
                    std::task::Poll::Ready(Some(Ok(packet))) => match packet {
                        crate::proto::Packet::ConnAck(crate::proto::ConnAck {
                            session_present,
                            return_code: crate::proto::ConnectReturnCode::Accepted,
                        }) => {
                            self.current_back_off = std::time::Duration::from_secs(0);

                            let reset_session = match client_id {
                                crate::proto::ClientId::ServerGenerated => true,
                                crate::proto::ClientId::IdWithCleanSession(id) => {
                                    *client_id = crate::proto::ClientId::IdWithExistingSession(
                                        std::mem::take(id),
                                    );
                                    true
                                }
                                crate::proto::ClientId::IdWithExistingSession(id) => {
                                    *client_id = crate::proto::ClientId::IdWithExistingSession(
                                        std::mem::take(id),
                                    );
                                    !session_present
                                }
                            };

                            *framed_state = FramedState::Connected {
                                new_connection: true,
                                reset_session,
                            };
                        }

                        crate::proto::Packet::ConnAck(crate::proto::ConnAck {
                            return_code: crate::proto::ConnectReturnCode::Refused(return_code),
                            ..
                        }) => {
                            log::warn!(
                                "could not connect to server: connection refused: {:?}",
                                return_code
                            );
                            *state = State::BeginBackOff;
                        }

                        packet => {
                            log::warn!("could not connect to server: expected to receive ConnAck but received {:?}", packet);
                            *state = State::BeginBackOff;
                        }
                    },

                    std::task::Poll::Ready(Some(Err(err))) => {
                        log::warn!("could not connect to server: {}", err);
                        *state = State::BeginBackOff;
                    }

                    std::task::Poll::Ready(None) => {
                        log::warn!("could not connect to server: connection closed by server");
                        *state = State::BeginBackOff;
                    }

                    std::task::Poll::Pending => return std::task::Poll::Pending,
                },

                State::Framed {
                    stream,
                    sink,
                    framed_state:
                        FramedState::Connected {
                            new_connection,
                            reset_session,
                        },
                    ..
                } => {
                    let result = Connected {
                        stream,
                        sink,
                        new_connection: *new_connection,
                        reset_session: *reset_session,
                    };
                    *new_connection = false;
                    *reset_session = false;
                    return std::task::Poll::Ready(result);
                }
            }
        }
    }
}

pub(super) struct Connected<'a, C>
where
    C: crate::io::Connector,
{
    pub(super) stream: &'a mut <C as crate::io::Connector>::PacketStream,
    pub(super) sink: &'a mut <C as crate::io::Connector>::PacketSink,
    pub(super) new_connection: bool,
    pub(super) reset_session: bool,
}
