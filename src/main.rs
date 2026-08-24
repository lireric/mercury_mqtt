use anyhow::{anyhow, Result};
use log::{debug, error, info, warn};
use log::LevelFilter;
use rumqttc::{AsyncClient, MqttOptions, QoS};
use serde::Serialize;
use std::env;
use std::time::Duration;
use tokio::time::sleep;

// --- Protocol Definitions (Mirroring C Structs) ---

#[repr(C, packed)]
#[derive(Clone, Copy)]
struct ModbusHeader {
    address: u8,
    command: u8,
}

#[repr(C, packed)]
#[derive(Clone, Copy)]
struct ReadParamCmd {
    address: u8,
    command: u8,
    param_id: u8,
    bwri: u8,
    crc: u16,
}

#[repr(C, packed)]
struct Result1b {
    address: u8,
    result: u8,
    crc: u16,
}

#[repr(C, packed)]
struct Result3b {
    address: u8,
    res: [u8; 3],
    crc: u16,
}

#[repr(C, packed)]
struct Result3x3b {
    address: u8,
    p1: [u8; 3],
    p2: [u8; 3],
    p3: [u8; 3],
    crc: u16,
}

#[repr(C, packed)]
struct Result4x3b {
    address: u8,
    sum: [u8; 3],
    p1: [u8; 3],
    p2: [u8; 3],
    p3: [u8; 3],
    crc: u16,
}

#[repr(C, packed)]
struct Result4x4b {
    address: u8,
    ap: [u8; 4],
    am: [u8; 4],
    rp: [u8; 4],
    rm: [u8; 4],
    crc: u16,
}

// --- Data Models for MQTT ---
struct PhaseDataRaw {
    p1: f32, p2: f32, p3: f32,
}

struct SumPhaseDataRaw {
    sum: f32, p1: f32, p2: f32, p3: f32,
}

struct PowerCounterRaw {
    ap: f32, am: f32, rp: f32, rm: f32,
}

#[derive(Debug, Serialize, Clone)]
struct PhaseData {
    p1: Option<f32>,
    p2: Option<f32>,
    p3: Option<f32>,
}

#[derive(Debug, Serialize, Clone)]
struct SumPhaseData {
    sum: Option<f32>,
    p1: Option<f32>,
    p2: Option<f32>,
    p3: Option<f32>,
}

#[derive(Debug, Serialize, Clone)]
struct PowerCounter {
    ap: Option<f32>,
    am: Option<f32>,
    rp: Option<f32>,
    rm: Option<f32>,
}

#[derive(Debug, Serialize, Clone)]
struct FullMeterSnapshot {
    voltage: PhaseData,
    current: PhaseData,
    cos_f: SumPhaseData,
    frequency: Option<f32>,
    phase_angles: PhaseData,
    active_power: SumPhaseData,
    reactive_power: SumPhaseData,
    total_consumed_kw: Option<f32>,
    tariff_day_kw: Option<f32>,
    tariff_night_kw: Option<f32>,
    yesterday_kw: Option<f32>,
    today_kw: Option<f32>,
}

// --- Hardware Driver ---

struct MercuryMeter {
    port: Box<dyn serialport::SerialPort>,
    address: u8,
}

impl MercuryMeter {
 fn new(path: &str, baud: u32, address: u8) -> Result<Self> {
        let port = serialport::new(path, baud)
            .timeout(Duration::from_millis(2000))
            .open()?;
        Ok(Self { port, address })
    }

    fn compute_crc(data: &[u8]) -> u16 {
        let mut crc: u16 = 0xFFFF;
        for &byte in data {
            crc ^= byte as u16;
            for _ in 0..8 {
                if (crc & 0x0001) != 0 {
                    crc >>= 1;
                    crc ^= 0xA001;
                } else {
                    crc >>= 1;
                }
            }
        }
        crc
    }

 async fn send_and_receive(&mut self, cmd_bytes: &[u8]) -> Result<Vec<u8>> {
    // 1. AGGRESSIVE CLEAR: Read everything currently in the buffer until it's empty
    let mut discard = [0u8; 128];
    loop {
        match self.port.read(&mut discard) {
            Ok(0) => break, // Buffer is empty
            Ok(_) => continue, // Keep reading until empty
            Err(_) => break, // Error or timeout, stop clearing
        }
    }

    // 2. Short pause to let the line settle
    sleep(Duration::from_millis(50)).await;

    // 3. Send the command
    self.port.write_all(cmd_bytes)?;
    
    // 4. IMPORTANT: Wait for the meter to process
    // Mercury meters can be slow on RS485. Let's use 250ms.
    sleep(Duration::from_millis(250)).await; 

    // 5. Read the response
    let mut buffer = vec![0u8; 256];
    let n = self.port.read(&mut buffer)?;
    if n == 0 {
        return Err(anyhow!("No response from meter"));
    }
    buffer.truncate(n);
    
    debug!("TX: {:02X?} | RX: {:02X?}", cmd_bytes, buffer);
    Ok(buffer)
}

    // Safer connection check without relying on struct casting
    async fn check_connection(&mut self) -> Result<()> {
        let mut cmd = vec![self.address, 0x00];
        let crc = Self::compute_crc(&cmd);
        cmd.push((crc & 0xFF) as u8);
        cmd.push(((crc >> 8) & 0xFF) as u8);

        let resp = self.send_and_receive(&cmd).await?;
        
        if resp.len() < 4 { return Err(anyhow!("Response too short")); }
        
        // Check CRC of response manually
        let calc_crc = Self::compute_crc(&resp[..resp.len()-2]);
        let resp_crc = ((resp[resp.len()-1] as u16) << 8) | (resp[resp.len()-2] as u16);
        
        if calc_crc != resp_crc {
            return Err(anyhow!("CRC Mismatch: {:04X} != {:04X}", calc_crc, resp_crc));
        }

        // Check result code (byte 1)
        let res_code = resp[1] & 0x0F;
        if res_code != 0 {
            return Err(anyhow!("Meter Error Code: {}", res_code));
        }
        Ok(())
    }

    fn decode_b3f_u(b: &[u8], factor: f32) -> f32 {
        if b.len() < 3 { return 0.0; }
        // Little Endian: [LSB, Mid, MSB]
        let val = ((b[0] as u32) & 0x3f) << 16 | ((b[2] as u32) << 8) | ((b[1] as u32));
        val as f32 / factor
    }
    // 4-byte Little Endian UNSIGNED (Use for Sum if Sum is unsigned)
    fn decode_b4f_u(b: &[u8], factor: f32) -> f32 {
        if b.len() < 4 { return 0.0; }
        let val = (b[2] as u32) | 
                  ((b[3] as u32) << 8) | 
                  ((b[0] as u32) << 16) | 
                  ((b[1] as u32) << 24);
        val as f32 / factor
    }

  async fn init(&mut self) ->  Result<()> {
    // 1. Build the command vector manually (Address, Function, ParamID, BWRI)
    let mut cmd = vec![
        self.address, // 0x00
        0x01,         // Function code
        0x01,         // 
        0x01,         // 
        0x01,         // 
        0x01,         // 
        0x01,         // 
        0x01,         // 
        0x01,         // 
    ];

    // 2. Calculate and append CRC (Modbus RTU style)
    let crc = Self::compute_crc(&cmd);
    cmd.push((crc & 0xFF) as u8);        // CRC Low
    cmd.push(((crc >> 8) & 0xFF) as u8); // CRC High

    // 3. Send and receive
    let resp = self.send_and_receive(&cmd).await?;

    // 4. SAFETY CHECK: If the meter sent an error, don't try to slice the data!
    if resp.len() < 4 {
        return Err(anyhow!("Response too short: {:02X?}", resp));
    }
    
    // Check if byte 1 is an error (e.g., 0x05)
    if resp[1] & 0x0F != 0 {
        return Err(anyhow!("Meter returned Error Code: {:02X?} (Bytes: {:02X?})", resp[1], resp));
    }

    Ok(())
}



    async fn get_u(&mut self) -> Result<PhaseDataRaw> {
        let mut cmd = vec![self.address, 0x08, 0x16, 0x11];
        let crc = Self::compute_crc(&cmd);
        cmd.push((crc & 0xFF) as u8);
        cmd.push(((crc >> 8) & 0xFF) as u8);

        let resp = self.send_and_receive(&cmd).await?;
        if resp.len() < 11 { return Err(anyhow!("Short RX for Voltage")); }

        let p1 = Self::decode_b3f_u(&resp[1..4], 100.0);
        let p2 =Self::decode_b3f_u(&resp[4..7], 100.0);
        let p3 = Self::decode_b3f_u(&resp[7..10], 100.0);
        Ok(PhaseDataRaw { p1, p2, p3 })
    }

    async fn get_i(&mut self) -> Result<PhaseDataRaw> {
        let mut cmd = ReadParamCmd { address: self.address, command: 0x08, param_id: 0x16, bwri: 0x21, crc: 0 };
        let bytes = self.prepare_cmd(&mut cmd);
        let resp = self.send_and_receive(&bytes).await?;

        if resp.len() < 11 { return Err(anyhow!("Short RX")); }

        let p1 = Self::decode_b3f_u(&resp[1..4], 1000.0);
        let p2 =Self::decode_b3f_u(&resp[4..7], 1000.0);
        let p3 = Self::decode_b3f_u(&resp[7..10], 1000.0);

        Ok(PhaseDataRaw { p1, p2, p3 })
    }    

    async fn get_cos_f(&mut self) -> Result<SumPhaseDataRaw> {
        let mut cmd = ReadParamCmd { address: self.address, command: 0x08, param_id: 0x16, bwri: 0x30, crc: 0 };
        let bytes = self.prepare_cmd(&mut cmd);
        let resp = self.send_and_receive(&bytes).await?;

        // Result4x3b: address(1) + sum(3) + p1(3) + p2(3) + p3(3) + crc(2) = 15 bytes
        if resp.len() < 14 {
            return Err(anyhow!("Response too short for CosF: {:02X?}", resp));
        }

        Ok(SumPhaseDataRaw {
        p1: Self::decode_b3f_u(&resp[1..4], 1000.0),
        p2: Self::decode_b3f_u(&resp[4..7], 1000.0),
        p3: Self::decode_b3f_u(&resp[7..10], 1000.0),
        sum: Self::decode_b3f_u(&resp[10..13], 1000.0),
        })
    }

    async fn get_f(&mut self) -> Result<f32> {
        let mut cmd = ReadParamCmd { address: self.address, command: 0x08, param_id: 0x16, bwri: 0x40, crc: 0 };
        let bytes = self.prepare_cmd(&mut cmd);
        let resp = self.send_and_receive(&bytes).await?;
        let f = ((resp[2] as u32) | ((resp[3] as u32) << 8)) as f32 / 100.0; 
        Ok(f)
    }

async fn get_p(&mut self) -> Result<SumPhaseDataRaw> {
    let mut cmd = ReadParamCmd { address: self.address, command: 0x08, param_id: 0x16, bwri: 0x00, crc: 0 };
    let bytes = self.prepare_cmd(&mut cmd);
    let resp = self.send_and_receive(&bytes).await?;

    // Corrected Slicing:
    // [0,1] Header, [2,3,4] Sum, [5,6,7] P1, [8,9,10] P2, [11,12,13] P3
    if resp.len() < 14 { return Err(anyhow!("Short RX")); }

    Ok(SumPhaseDataRaw {
        p1: Self::decode_b3f_u(&resp[1..4], 100.0),
        p2: Self::decode_b3f_u(&resp[4..7], 100.0),
        p3: Self::decode_b3f_u(&resp[7..10], 100.0),
        sum: Self::decode_b3f_u(&resp[10..13], 100.0),
    })
}

    async fn get_s(&mut self) -> Result<SumPhaseDataRaw> {
        let mut cmd = ReadParamCmd { address: self.address, command: 0x08, param_id: 0x16, bwri: 0x08, crc: 0 };
        let bytes = self.prepare_cmd(&mut cmd);
        let resp = self.send_and_receive(&bytes).await?;

        if resp.len() < 14 {
            return Err(anyhow!("Response too short for Reactive Power: {:02X?}", resp));
        }

        Ok(SumPhaseDataRaw {
        p1: Self::decode_b3f_u(&resp[1..4], 100.0),   // Sum is 3 bytes
        p2: Self::decode_b3f_u(&resp[4..7], 100.0),   // P1 is 3 bytes
        p3: Self::decode_b3f_u(&resp[7..10], 100.0),  // P2 is 3 bytes
        sum: Self::decode_b3f_u(&resp[10..13], 100.0), // P3 is 3 bytes
        })
    }


   async fn get_w(&mut self, period_id: u8, month: u8, tariff: u8) -> Result<PowerCounterRaw> {
        let mut cmd = ReadParamCmd {
            address: self.address,
            command: 0x05,
            param_id: (period_id << 4) | (month & 0x0F),
            bwri: tariff,
            crc: 0,
        };
        let bytes = self.prepare_cmd(&mut cmd);
        let resp = self.send_and_receive(&bytes).await?;

        // Based on Python Log: [0,1] Header, [2,3,4,5] AP, [6,7,8,9] AM, [10,11,12,13] RP, [14,15,16,17] RM
        if resp.len() < 18 { return Err(anyhow!("Short RX")); }

        Ok(PowerCounterRaw {
            // CHANGED: Use decode_b4f_u instead of decode_b4f_s
            ap: Self::decode_b4f_u(&resp[1..5], 1000.0), 
            am: Self::decode_b4f_u(&resp[5..9], 1000.0),
            rp: Self::decode_b4f_u(&resp[9..13], 1000.0),
            rm: Self::decode_b4f_u(&resp[13..17], 1000.0),
        })

    }

fn prepare_cmd(&self, cmd: &ReadParamCmd) -> Vec<u8> {
    let mut bytes = vec![
        cmd.address,
        cmd.command,
        cmd.param_id,
        cmd.bwri,
    ];
    // If the command is a ReadParamCmd, it's 6 bytes total (4 data + 2 CRC)
    // If it's a different command, you need to handle it.
    // For ReadParamCmd:
    let crc = Self::compute_crc(&bytes);
    bytes.push((crc & 0xFF) as u8);
    bytes.push(((crc >> 8) & 0xFF) as u8);
    bytes
}

}

async fn publish_discovery_configs(client: &rumqttc::AsyncClient, device_id: &str, mqtt_acc: &str) -> Result<()> {
    // The base_topic is now just the component prefix: homeassistant/sensor/
    let base_topic = "homeassistant/sensor";
    
    let device_info = serde_json::json!({
        "identifiers": [device_id],
        "name": format!("Mercury Meter {}", device_id),
        "model": "Mercury 236",
        "manufacturer": "Elster"
    });

    let sensor_definitions = vec![
        // Voltages
        ("volt_p1", "Voltage Phase 1", "V", "voltage"),
        ("volt_p2", "Voltage Phase 2", "V", "voltage"),
        ("volt_p3", "Voltage Phase 3", "V", "voltage"),
        // Currents
        ("amp_p1", "Current Phase 1", "A", "current"),
        ("amp_p2", "Current Phase 2", "A", "current"),
        ("amp_p3", "Current Phase 3", "A", "current"),
        // Active Power
        ("pow_p1", "Active Power Phase 1", "W", "power"),
        ("pow_p2", "Active Power Phase 2", "W", "power"),
        ("pow_p3", "Active Power Phase 3", "W", "power"),
        ("pow_sum", "Total Active Power", "W", "power"),
        // Frequency
        ("freq", "Frequency", "Hz", "frequency"),
        // Cos Phi (Power Factor)
        ("cos_f_sum", "Total Power Factor", "", "power_factor"),
        ("cos_f_p1", "Power Factor Phase 1", "", "power_factor"),
        ("cos_f_p2", "Power Factor Phase 2", "", "power_factor"),
        ("cos_f_p3", "Power Factor Phase 3", "", "power_factor"),
        // Reactive Power
        ("reac_p1", "Reactive Power Phase 1", "var", "reactive_power"),
        ("reac_p2", "Reactive Power Phase 2", "var", "reactive_power"),
        ("reac_p3", "Reactive Power Phase 3", "var", "reactive_power"),
        ("reac_sum", "Total Reactive Power", "var", "reactive_power"),
        // Energy
        ("energy_total", "Total Consumed Energy", "kWh", "energy"),
        ("energy_day", "Daily Consumed Energy", "kWh", "energy"),
        ("energy_night", "Nightly Consumed Energy", "kWh", "energy"),
        ("energy_yesterday", "Yesterday Consumed Energy", "kWh", "energy"),
        ("energy_today", "Today Consumed Energy", "kWh", "energy"),
    ];

    for (suffix, name, unit, dev_class) in sensor_definitions {
        // Flattened topic: homeassistant/sensor/{device_id}_{suffix}/config
        let topic = format!("{}/{}_{}/config", base_topic, device_id, suffix);
        
        // The stat_t remains your custom data path (this part was correct)
        let stat_t = format!("/ssn/acc/{}/obj/power/device/{}/{}", mqtt_acc, device_id, suffix);
        
        let payload = serde_json::json!({
            "name": format!("{} {}", name, device_id),
            "unique_id": format!("{}_{}", device_id, suffix),
            "stat_t": stat_t,
            "unit_of_measurement": unit,
            "dev_cla": dev_class,
            "device": device_info
        });

        client.publish(topic, QoS::AtLeastOnce, true, payload.to_string()).await?;
    }

    info!("Discovery configurations published.");
    Ok(())
}
// --- Main Application Logic ---

#[tokio::main]
async fn main() -> Result<()> {
    dotenv::dotenv().ok();
    // env_logger::init();
    let log_level = match env::var("LOG_LEVEL").unwrap_or_else(|_| "info".to_string()).as_str() {
        "debug" => LevelFilter::Debug,
        "info" => LevelFilter::Info,
        "warn" => LevelFilter::Warn,
        "error" => LevelFilter::Error,
        _ => LevelFilter::Info,
    };

    env_logger::Builder::from_default_env()
        .filter_level(log_level)
        .init();

    info!("Mercury monitor is starting..");

    // Config from Envs
    let serial_path = env::var("SERIAL_PORT").unwrap_or_else(|_| "/dev/ttyUSB0".to_string());
    let baudrate: u32 = env::var("BAUD_RATE").unwrap_or("9600".to_string()).parse()?;
    let mqtt_host = env::var("MQTT_HOST").unwrap_or("192.168.3.150".to_string());
    let mqtt_port: u16 = env::var("MQTT_PORT").unwrap_or("1883".to_string()).parse()?;
    let mqtt_user = env::var("MQTT_USER").unwrap_or_else(|_| "mosquitto".to_string());
    let mqtt_pass = env::var("MQTT_PASSWORD").unwrap_or_else(|_| "test".to_string());
    let mqtt_acc = env::var("ACCOUNT_ID").unwrap_or("1".to_string());
    let device_id = env::var("DEVICE_ID").unwrap_or("mercury_02".to_string());
    let poll_interval: u64 = env::var("POLL_INTERVAL").unwrap_or("60".to_string()).parse()?;

    // MQTT Setup
    let mut mqttoptions = MqttOptions::new(&device_id, mqtt_host.clone(), mqtt_port.clone());
    mqttoptions.set_keep_alive(Duration::from_secs(5));

    if !mqtt_user.is_empty() || !mqtt_pass.is_empty() {
        mqttoptions.set_credentials(mqtt_user, mqtt_pass);
        info!("MQTT credentials configured");
    } else {
        warn!("MQTT credentials does not configured!");
    }

    let (client, mut eventloop) = AsyncClient::new(mqttoptions, 10);

    // Spawn MQTT event loop task
    let mqtt_client_clone = client.clone();
    let mqtt_host_clone = mqtt_host.clone();
    let mqtt_port_clone = mqtt_port;
    tokio::spawn(async move {
        loop {
            if let Err(e) = eventloop.poll().await {
                error!("MQTT Connection error: {}, host={}, port={}", e, mqtt_host_clone, mqtt_port_clone);
                sleep(Duration::from_secs(5)).await;
            }
        }
    });

    // Hardware Setup
    let mut meter = MercuryMeter::new(&serial_path, baudrate, 0)?;
    info!("Starting Mercury 236 monitor on {}", serial_path);

    // Home Assistant Discovery
    publish_discovery_configs(&client, &device_id, &mqtt_acc).await?;


    // Initial Connection Check
    if let Err(e) = meter.check_connection().await {
        error!("Initial connection check failed: {}", e);
        // In production, you might want to retry here instead of exiting
    } else {
        info!("Connection verified.");
        let _ = meter.init().await;
    }

    // --- Main Polling Loop ---
    info!("Entering main polling loop. Interval: {}s", poll_interval);
    

     loop {
        let s = collect_snapshot(&mut meter).await;
        debug!("FullMeterSnapshot: {:?}", s);

        let base_topic = format!("/ssn/acc/{}/obj/power/device/{}/", mqtt_acc, device_id);
        
        // 2. Define a helper to publish only if the value is Some(val)
        // This prevents sending "None" or "0.0" when a sensor is actually failing.
        let pub_val = |suffix: &str, val: Option<f32>| {
            let client = client.clone();
            // let topic = format!("{}{}", base_topic, suffix);
            let topic_suffix = suffix.to_string();
            let base_topic_owned = base_topic.clone(); // Clone base_topic to move it in

        tokio::spawn(async move {
            let topic = format!("{}{}", base_topic_owned, topic_suffix);
            let payload = match val {
                Some(v) => format!("{}", v), //format!("{:.2}", v),
                None => "unavailable".to_string(),
            };
            debug!("topic={}", topic);
            debug!("payload={}", payload);

            if let Err(e) = client.publish(topic, QoS::AtMostOnce, false, payload).await {
                error!("MQTT Publish Error: {}", e);
            }
        });
    };

        // 3. Dispatch all readings
        // Voltages
        pub_val("volt_p1", s.voltage.p1);
        pub_val("volt_p2", s.voltage.p2);
        pub_val("volt_p3", s.voltage.p3);

        // Amperage
        pub_val("amp_p1", s.current.p1);
        pub_val("amp_p2", s.current.p2);
        pub_val("amp_p3", s.current.p3);

        // Active Power
        pub_val("pow_p1", s.active_power.p1);
        pub_val("pow_p2", s.active_power.p2);
        pub_val("pow_p3", s.active_power.p3);
        pub_val("pow_sum", s.active_power.sum); // <--- ADDED THIS

        // Frequency
        pub_val("freq", s.frequency);

        // Cos Phi
        pub_val("cos_f_sum", s.cos_f.sum);
        pub_val("cos_f_p1", s.cos_f.p1);
        pub_val("cos_f_p2", s.cos_f.p2);
        pub_val("cos_f_p3", s.cos_f.p3);

        // Reactive Power
        pub_val("reac_p1", s.reactive_power.p1);
        pub_val("reac_p2", s.reactive_power.p2);
        pub_val("reac_p3", s.reactive_power.p3);
        pub_val("reac_sum", s.reactive_power.sum); // <--- ADDED THIS

        // Energy
        pub_val("energy_total", s.total_consumed_kw);
        pub_val("energy_day", s.tariff_day_kw);
        pub_val("energy_night", s.tariff_night_kw);
        pub_val("energy_yesterday", s.yesterday_kw);
        pub_val("energy_today", s.today_kw);

        // 4. Logging summary
        match (s.voltage.p1, s.current.p1) {
            (Some(v), Some(i)) => info!("Snapshot published: {:.1}V, {:.2}A", v, i),
            _ => warn!("Snapshot partially failed: some critical values are missing"),
        }

        // 5. Wait for next interval
        sleep(Duration::from_secs(poll_interval)).await;
    }        
}

async fn collect_snapshot(meter: &mut MercuryMeter) -> FullMeterSnapshot {
    // Helper to wrap calls and log errors without stopping the whole process
    // This prevents one bad command from ruining the entire cycle.
    macro_rules! try_get {
        ($call:expr) => {
            match $call.await {
                Ok(val) => Some(val),
                Err(e) => {
                    error!("Failed to fetch parameter: {}", e);
                    None
                }
            }
        };
    }

    // 1. Fetch complex types
    let voltage = try_get!(meter.get_u()).map(|v| PhaseData { 
        p1: Some(v.p1), p2: Some(v.p2), p3: Some(v.p3) 
    }).unwrap_or(PhaseData { p1: None, p2: None, p3: None });

    let current = try_get!(meter.get_i()).map(|v| PhaseData { 
        p1: Some(v.p1), p2: Some(v.p2), p3: Some(v.p3) 
    }).unwrap_or(PhaseData { p1: None, p2: None, p3: None });

    let cos_f = try_get!(meter.get_cos_f()).map(|v| SumPhaseData { 
        sum: Some(v.sum), p1: Some(v.p1), p2: Some(v.p2), p3: Some(v.p3) 
    }).unwrap_or(SumPhaseData { sum: None, p1: None, p2: None, p3: None });

    let active_power = try_get!(meter.get_p()).map(|v| SumPhaseData { 
        sum: Some(v.sum), p1: Some(v.p1), p2: Some(v.p2), p3: Some(v.p3) 
    }).unwrap_or(SumPhaseData { sum: None, p1: None, p2: None, p3: None });

    let reactive_power = try_get!(meter.get_s()).map(|v| SumPhaseData { 
        sum: Some(v.sum), p1: Some(v.p1), p2: Some(v.p2), p3: Some(v.p3) 
    }).unwrap_or(SumPhaseData { sum: None, p1: None, p2: None, p3: None });

    // 2. Fetch single values
    let frequency = try_get!(meter.get_f());

    // 3. Fetch Energy/Tariff (using the helper for the struct)
let w_total     = try_get!(meter.get_w(0, 0, 0)).and_then(|w| Some(w.ap));
let w_day       = try_get!(meter.get_w(0, 0, 1)).and_then(|w| Some(w.ap));
let w_night     = try_get!(meter.get_w(0, 0, 2)).and_then(|w| Some(w.ap));
let w_yesterday = try_get!(meter.get_w(5, 0, 0)).and_then(|w| Some(w.ap));
let w_today     = try_get!(meter.get_w(4, 0, 0)).and_then(|w| Some(w.ap));
    FullMeterSnapshot {
        voltage,
        current,
        cos_f,
        frequency,
        phase_angles: PhaseData { p1: None, p2: None, p3: None }, // Placeholder
        active_power,
        reactive_power,
        total_consumed_kw: w_total,
        tariff_day_kw: w_day,
        tariff_night_kw: w_night,
        yesterday_kw: w_yesterday,
        today_kw: w_today,
    }
}