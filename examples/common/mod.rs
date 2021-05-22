#[cfg(any(
    feature = "transport-smol",
    feature = "transport-tokio",
))]
pub(crate) mod transport;

pub(crate) fn init<Options>(example_name: &str) -> Options where Options: structopt::StructOpt {
    env_logger::Builder::from_env(env_logger::Env::new().filter_or(
        "MQTT3_LOG",
        &format!("mqtt3=info,{example_name}=info", example_name = example_name),
    ))
    .init();

    structopt::StructOpt::from_args()
}

#[allow(dead_code)]
pub(crate) fn duration_from_secs_str(
    s: &str,
) -> Result<std::time::Duration, <u64 as std::str::FromStr>::Err> {
    Ok(std::time::Duration::from_secs(s.parse()?))
}

#[allow(dead_code)]
pub(crate) fn qos_from_str(s: &str) -> Result<mqtt3::proto::QoS, String> {
    match s {
        "0" | "AtMostOnce" => Ok(mqtt3::proto::QoS::AtMostOnce),
        "1" | "AtLeastOnce" => Ok(mqtt3::proto::QoS::AtLeastOnce),
        "2" | "ExactlyOnce" => Ok(mqtt3::proto::QoS::ExactlyOnce),
        s => Err(format!(
            "unrecognized QoS {:?}: must be one of 0, 1, 2, AtMostOnce, AtLeastOnce, ExactlyOnce",
            s
        )),
    }
}

pub(crate) const PACKET_STATS_WINDOW_SIZE: usize = 60;

#[allow(dead_code)]
pub(crate) struct PacketStats {
    start_time: std::time::Instant,
    current: u128,
    history: heapless::HistoryBuffer<u128, PACKET_STATS_WINDOW_SIZE>,
}

impl PacketStats {
    #[allow(dead_code)]
    pub(crate) fn count(&mut self, num: usize) {
        self.current += num as u128;

        let now = std::time::Instant::now();
        let elapsed = now.duration_since(self.start_time);
        if elapsed > std::time::Duration::from_secs(1) {
            self.history.write(self.current);

            log::info!(
                "{:>8} | 1s average: {:>8}/s) | {}s average: {:>8}/s{}",
                self.current,
                self.current * 1_000_000 / elapsed.as_micros(),
                PACKET_STATS_WINDOW_SIZE,
                self.history.as_slice().iter().sum::<u128>() * 1_000_000 / (self.history.len() as u128) / elapsed.as_micros(),
                if self.history.len() < PACKET_STATS_WINDOW_SIZE { " (?)" } else { "" },
            );

            self.current = 0;
            self.start_time = now;
        }
    }
}

impl Default for PacketStats {
    fn default() -> Self {
        PacketStats {
            start_time: std::time::Instant::now(),
            current: 0,
            history: heapless::HistoryBuffer::new(),
        }
    }
}
