/*!
 * This crate contains an implementation of an MQTT client and server.
 */

#![deny(rust_2018_idioms, warnings)]
#![deny(clippy::all, clippy::pedantic)]
#![allow(
    clippy::default_trait_access,
    clippy::large_enum_variant,
    clippy::let_underscore_drop,
    clippy::let_unit_value,
    clippy::missing_errors_doc,
    clippy::module_name_repetitions,
    clippy::must_use_candidate,
    clippy::pub_enum_variant_names,
    clippy::too_many_arguments,
    clippy::too_many_lines,
)]

#[allow(clippy::declare_interior_mutable_const)]
pub const PROTOCOL_NAME: proto::ByteStr = proto::ByteStr::from_static("MQTT");

pub const PROTOCOL_LEVEL: u8 = 0x04;

#[cfg(feature = "client")]
mod client;
#[cfg(feature = "client")]
pub use client::{
    Client, ConnectionError, Error, Event, IoSource, PublishError, PublishHandle,
    ReceivedPublication, ShutdownError, ShutdownHandle, SubscriptionUpdateEvent,
    UpdateSubscriptionError, UpdateSubscriptionHandle,
};

#[cfg(feature = "server")]
mod server;
#[cfg(feature = "server")]
pub use server::{
    Server,
};

#[cfg(any(
    feature = "client",
    feature = "server",
))]
mod logging_framed;

pub mod proto;
