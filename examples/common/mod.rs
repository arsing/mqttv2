use std::{thread, time::Duration};

use systemstat::{saturating_sub_bytes, Platform, System};

#[cfg(any(feature = "transport-smol", feature = "transport-tokio",))]
pub(crate) mod transport;

pub(crate) fn init<Options>(example_name: &str) -> Options
where
    Options: structopt::StructOpt,
{
    env_logger::Builder::from_env(env_logger::Env::new().filter_or(
        "MQTT3_LOG",
        &format!(
            "mqtt3=info,{example_name}=info",
            example_name = example_name
        ),
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

type History = heapless::HistoryBuffer<u128, PACKET_STATS_WINDOW_SIZE>;

#[allow(dead_code)]
pub(crate) struct PacketStats {
    start_time: std::time::Instant,
    current: u128,
    bytes: u128,
    message_history: History,
    byte_history: History,
}

impl PacketStats {
    #[allow(dead_code)]
    pub(crate) fn log_metrics(&mut self, num: usize, bytes: usize) {
        self.current += num as u128;
        self.bytes += bytes as u128;

        let now = std::time::Instant::now();
        let elapsed = now.duration_since(self.start_time);
        if elapsed > std::time::Duration::from_secs(1) {
            // Message metrics
            self.message_history.write(self.current);

            log_metric(
                "messages processed",
                self.current,
                elapsed.as_micros(),
                &self.message_history,
            );

            self.current = 0;

            // Bytes metrics
            self.byte_history.write(self.bytes);

            log_metric(
                "bytes processed",
                self.bytes,
                elapsed.as_micros(),
                &self.byte_history,
            );

            self.bytes = 0;
            self.start_time = now;

            // System metrics
            log_sys_metrics();
        }
    }
}

impl Default for PacketStats {
    fn default() -> Self {
        PacketStats {
            start_time: std::time::Instant::now(),
            current: 0,
            bytes: 0,
            message_history: History::new(),
            byte_history: History::new(),
        }
    }
}

fn log_metric(metric_name: &str, metric_value: u128, elapsed_micros: u128, history: &History) {
    log::info!(
        "{}: {:>8} | 1s average: {:>8}/s) | {}s average: {:>8}/s{}",
        metric_name,
        metric_value,
        metric_value * 1_000_000 / elapsed_micros,
        PACKET_STATS_WINDOW_SIZE,
        history.as_slice().iter().sum::<u128>() * 1_000_000
            / (history.len() as u128)
            / elapsed_micros,
        if history.len() < PACKET_STATS_WINDOW_SIZE {
            " (?)"
        } else {
            ""
        },
    );
}

fn log_sys_metrics() {
    let sys = System::new();

    match sys.networks() {
        Ok(netifs) => {
            log::info!("Networks:");
            for netif in netifs.values() {
                log::info!("{} ({:?})", netif.name, netif.addrs);
            }
        }
        Err(x) => log::info!("Networks: error: {}", x),
    }

    match sys.networks() {
        Ok(netifs) => {
            log::info!("Network interface statistics:");
            for netif in netifs.values() {
                log::info!(
                    "{} statistics: ({:?})",
                    netif.name,
                    sys.network_stats(&netif.name)
                );
            }
        }
        Err(x) => log::info!("Networks: error: {}", x),
    }

    match sys.memory() {
        Ok(mem) => log::info!(
            "Memory: {} used / {} ({} bytes) total ({:?})",
            saturating_sub_bytes(mem.total, mem.free),
            mem.total,
            mem.total.as_u64(),
            mem.platform_memory
        ),
        Err(x) => log::info!("Memory: error: {}", x),
    }

    match sys.load_average() {
        Ok(loadavg) => log::info!(
            "Load average: {} {} {}",
            loadavg.one,
            loadavg.five,
            loadavg.fifteen
        ),
        Err(x) => log::info!("Load average: error: {}", x),
    }

    match sys.cpu_load_aggregate() {
        Ok(cpu) => {
            log::info!("Measuring CPU load...");
            thread::sleep(Duration::from_secs(1));
            let cpu = cpu.done().unwrap();
            log::info!(
                "CPU load: {}% user, {}% nice, {}% system, {}% intr, {}% idle ",
                cpu.user * 100.0,
                cpu.nice * 100.0,
                cpu.system * 100.0,
                cpu.interrupt * 100.0,
                cpu.idle * 100.0
            );
        }
        Err(x) => log::info!("CPU load: error: {}", x),
    }

    match sys.socket_stats() {
        Ok(stats) => log::info!("System socket statistics: {:?}", stats),
        Err(x) => log::info!("System socket statistics: error: {}", x.to_string()),
    }
}
