use std::{
    collections::{BTreeMap, HashMap, HashSet},
    io::Read,
    net::SocketAddr,
    str::FromStr,
    sync::{Arc, RwLock},
};

use anyhow::{Context, Result};
use axum::{
    extract::{
        ws::{Message, WebSocket},
        ConnectInfo, State, WebSocketUpgrade,
    },
    headers,
    response::IntoResponse,
    routing::get,
    TypedHeader,
};
use flate2::read::GzDecoder;
use rijksdriehoek::rijksdriehoek_to_wgs84;
use serde::{Deserialize, Deserializer, Serialize};
use tower_http::trace::{DefaultMakeSpan, TraceLayer};
use tracing_subscriber::{prelude::__tracing_subscriber_SubscriberExt, util::SubscriberInitExt};
use zeromq::{Socket, SocketRecv};

#[derive(Debug, Deserialize)]
struct VvTmPush {
    #[serde(rename = "Timestamp")]
    timestamp: String,
    #[serde(default)]
    #[serde(rename = "KV6posinfo")]
    posinfo: Vec<Kv6PosInfo>,
}

#[derive(Debug, Deserialize)]
struct Kv6PosInfo {
    #[serde(default)]
    #[serde(rename = "ARRIVAL")]
    arrivals: Vec<Kv6Position>,
    #[serde(default)]
    #[serde(rename = "DEPARTURE")]
    departures: Vec<Kv6Position>,
    #[serde(default)]
    #[serde(rename = "ONROUTE")]
    onroute: Vec<Kv6Position>,
    #[serde(default)]
    #[serde(rename = "ONSTOP")]
    onstop: Vec<Kv6Position>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "kebab-case")]
struct Kv6Position {
    journeynumber: Option<i32>,
    userstopcode: Option<String>,
    vehiclenumber: Option<String>,
    punctuality: Option<String>,
    rd_x: Option<f64>,
    rd_y: Option<f64>,
}

#[derive(Debug, Serialize, Clone)]
struct VehiclePosition {
    timestamp: String,
    journey_number: Option<i32>,
    user_stop_code: Option<String>,
    vehicle_number: Option<String>,
    punctuality: Option<String>,
    lat: f64,
    lon: f64,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
struct Passtime {
    data_owner_code: String,
    operation_date: String,
    line_planning_number: String,
    journey_number: i32,
    user_stop_order_number: i32,
    user_stop_code: String,
    journey_pattern_code: String,
    line_direction: i32,
    last_update_time_stamp: String,
    destination_code: String,
    #[serde(deserialize_with = "bool_from_int")]
    is_timing_stop: bool,
    expected_arrival_time: String,
    expected_departure_time: String,
    trip_stop_status: String,
    timing_point_code: String,
    journey_stop_type: String,
    target_arrival_time: String,
    target_departure_time: String,
    #[serde(deserialize_with = "option_from_null")]
    recorded_arrival_time: Option<String>,
    #[serde(deserialize_with = "option_from_null")]
    recorded_departure_time: Option<String>,
    detected_user_stop_code: String,
    distance_since_detected_user_stop: i32,
    #[serde(deserialize_with = "option_from_null")]
    #[serde(rename = "Detected_RD_X")]
    detected_rd_x: Option<String>,
    #[serde(deserialize_with = "option_from_null")]
    #[serde(rename = "Detected_RD_Y")]
    detected_rd_y: Option<String>,
    #[serde(deserialize_with = "option_from_null")]
    vehicle_number: Option<i32>,
    #[serde(deserialize_with = "option_from_null")]
    line_dest_icon: Option<String>,
    line_dest_color: String,
    line_dest_text_color: String,
}

fn bool_from_int<'de, D>(deserializer: D) -> Result<bool, D::Error>
where
    D: Deserializer<'de>,
{
    let s = String::deserialize(deserializer)?;
    Ok(s == "1")
}

fn option_from_null<'de, D, T>(deserializer: D) -> Result<Option<T>, D::Error>
where
    D: Deserializer<'de>,
    T: FromStr,
    <T as FromStr>::Err: std::fmt::Display,
{
    let s = String::deserialize(deserializer)?;
    if s == "\\0" {
        Ok(None)
    } else {
        Ok(Some(T::from_str(&s).map_err(serde::de::Error::custom)?))
    }
}

type VehiclePositionMap = Arc<RwLock<BTreeMap<i32, VehiclePosition>>>;
type PasstimeMap = Arc<RwLock<BTreeMap<i32, Passtime>>>;

#[derive(Debug, Clone)]
struct RouterState {
    vehicle_positions: VehiclePositionMap,
    passtimes: PasstimeMap,
    live_tx: tokio::sync::broadcast::Sender<VehiclePosition>,
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::registry()
        .with(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "ovlive=debug,tower_http=debug".into()),
        )
        .with(tracing_subscriber::fmt::layer())
        .init();

    let vehicle_positions: VehiclePositionMap = Default::default();
    let passtimes: PasstimeMap = Default::default();
    let (live_tx, _) = tokio::sync::broadcast::channel::<VehiclePosition>(64);

    tokio::spawn({
        let vehicle_positions = vehicle_positions.clone();
        let live_tx = live_tx.clone();
        async move {
            let mut socket = zeromq::SubSocket::new();
            socket
                .connect("tcp://pubsub.ndovloket.nl:7658")
                .await
                .context("Failed to connect")
                .unwrap();

            tracing::info!("Connected to BISON");

            socket.subscribe("/QBUZZ/KV6posinfo").await.unwrap();
            socket.subscribe("/QBUZZ/KV15messages").await.unwrap();
            socket.subscribe("/QBUZZ/KV17cvlinfo").await.unwrap();

            loop {
                let res = socket.recv().await.unwrap();

                let mut decoder = GzDecoder::new(&res.get(1).unwrap()[..]);
                let mut s = String::new();
                decoder.read_to_string(&mut s).unwrap();

                let res = quick_xml::de::from_str::<VvTmPush>(&s).unwrap();

                for pos in res
                    .posinfo
                    .iter()
                    .flat_map(|p| [&p.arrivals, &p.departures, &p.onroute, &p.onstop])
                    .flatten()
                {
                    if let Some(journey_number) = pos.journeynumber {
                        let (Some(rd_x), Some(rd_y)) = (pos.rd_x, pos.rd_y) else { continue };
                        let (lat, lon) = rijksdriehoek_to_wgs84(rd_x, rd_y);

                        let vehicle_position = VehiclePosition {
                            timestamp: res.timestamp.clone(),
                            journey_number: pos.journeynumber,
                            user_stop_code: pos.userstopcode.clone(),
                            vehicle_number: pos.vehiclenumber.clone(),
                            punctuality: pos.punctuality.clone(),
                            lat,
                            lon,
                        };

                        let _ = live_tx.send(vehicle_position.clone());

                        vehicle_positions
                            .write()
                            .unwrap()
                            .insert(journey_number, vehicle_position);

                        eprint!(".");
                    }
                }
            }
        }
    });

    tokio::spawn({
        let passtimes = passtimes.clone();
        async move {
            let mut socket = zeromq::SubSocket::new();
            socket
                .connect("tcp://pubsub.ndovloket.nl:7817")
                .await
                .context("Failed to connect")
                .unwrap();

            tracing::info!("Connected to KV78Turbo");

            socket.subscribe("/GOVI/KV8").await.unwrap();

            let mut seen_keys = HashSet::<String>::new();

            loop {
                let res = socket.recv().await.unwrap();

                let mut decoder = GzDecoder::new(&res.get(1).unwrap()[..]);
                let mut s = String::new();
                decoder.read_to_string(&mut s).unwrap();

                #[derive(Debug)]
                struct RecordHeader<'a> {
                    record_type: &'a str,
                    data_owner: &'a str,
                    timestamp: &'a str,
                }

                let mut parts = s.split("\r\n\\T");
                let header = parts.next().unwrap();

                let mut header = header.trim_start_matches(r"\G").split('|');
                let header = RecordHeader {
                    record_type: header.next().unwrap(),
                    data_owner: header.nth(1).unwrap(),
                    timestamp: header.nth(4).unwrap(),
                };

                if header.data_owner != "QBUZZ" {
                    continue;
                }

                let tables = parts
                    .map(|table| {
                        let mut table = table.split("\r\n\\L");
                        let name = table.next().unwrap().split('|').next().unwrap();
                        let data = table.next().unwrap();

                        (name, data)
                    })
                    .collect::<HashMap<&str, &str>>();

                for &key in tables.keys() {
                    if !seen_keys.contains(&key.to_owned()) {
                        seen_keys.insert(key.to_owned());
                        println!("New key: {key}");
                        let value = tables.get(key).unwrap();

                        let res = std::fs::write(format!("kv78t-{key}.csv"), value);
                        dbg!(res);
                    }
                }

                if let Some(table) = tables.get("DATEDPASSTIME") {
                    let mut rdr = csv::ReaderBuilder::new()
                        .delimiter(b'|')
                        .has_headers(true)
                        .from_reader(table.as_bytes());

                    for result in rdr
                        .deserialize::<Passtime>()
                        .map(|r| handle_err(r, table.as_bytes()))
                        .filter_map(Result::ok)
                    {
                        passtimes
                            .write()
                            .unwrap()
                            .insert(result.journey_number, result);

                        eprint!("'");
                    }
                }

                fn handle_err<T>(r: csv::Result<T>, input: &[u8]) -> csv::Result<T> {
                    if let Err(ref e) = r {
                        if let Some(pos) = e.position() {
                            eprintln!("{e:?}");
                            eprintln!(
                                "Error at byte {}: {:?}",
                                pos.byte(),
                                String::from_utf8_lossy(&input[(pos.byte() + 1) as usize..])
                                    .lines()
                                    .next()
                                    .unwrap()
                            );
                        }
                    }
                    r
                }
            }
        }
    });

    tokio::spawn(async move {
        let app = axum::Router::<RouterState>::new()
            .route("/ws", get(ws_handler))
            .layer(
                TraceLayer::new_for_http()
                    .make_span_with(DefaultMakeSpan::default().include_headers(true)),
            )
            .with_state(RouterState {
                vehicle_positions: vehicle_positions.clone(),
                passtimes: passtimes.clone(),
                live_tx: live_tx.clone(),
            });

        // let listener = TcpListener::bind("0.0.0.0:9498").await.unwrap();

        let addr = SocketAddr::from_str("0.0.0.0:9498").unwrap();
        tracing::debug!("listening on {}", addr);

        axum::Server::bind(&addr)
            .serve(app.into_make_service_with_connect_info::<SocketAddr>())
            .await
            .unwrap();
    })
    .await
    .unwrap();

    Ok(())
}

/// The handler for the HTTP request (this gets called when the HTTP GET lands at the start
/// of websocket negotiation). After this completes, the actual switching from HTTP to
/// websocket protocol will occur.
/// This is the last point where we can extract TCP/IP metadata such as IP address of the client
/// as well as things from HTTP headers such as user-agent of the browser etc.
async fn ws_handler(
    ws: WebSocketUpgrade,
    user_agent: Option<TypedHeader<headers::UserAgent>>,
    ConnectInfo(addr): ConnectInfo<SocketAddr>,
    state: State<RouterState>,
) -> impl IntoResponse {
    let user_agent = if let Some(TypedHeader(user_agent)) = user_agent {
        user_agent.to_string()
    } else {
        String::from("Unknown browser")
    };
    println!("`{user_agent}` at {addr} connected.");

    // finalize the upgrade process by returning upgrade callback.
    // we can customize the callback by sending additional info such as address.
    ws.on_upgrade(move |socket| async move {
        if let Err(e) = handle_socket(socket, addr, state.0).await {
            tracing::error!("Error in websocket handler: {:?}", e);
        }
    })
}

#[derive(Debug, Deserialize)]
enum WsCommand {
    SetSubscriptions { journey_numbers: Vec<i32> },
}

#[derive(Debug, Clone, Serialize)]
enum WsResponse {
    VehiclePosition(VehiclePosition),
}

async fn handle_socket(mut socket: WebSocket, who: SocketAddr, state: RouterState) -> Result<()> {
    tracing::info!("New websocket connection from {}", who);

    loop {
        let msg = socket
            .recv()
            .await
            .transpose()
            .context("Failed to receive message")?;

        let Some(msg) = msg else { return Ok(()) };

        let s = msg.to_text().context("Message was not text")?;

        let cmd = serde_json::from_str::<WsCommand>(s).context("Failed to parse JSON")?;

        match cmd {
            WsCommand::SetSubscriptions { journey_numbers } => {
                let mut messages = vec![];

                {
                    let vehicle_positions = state.vehicle_positions.read().unwrap();

                    for sub in &journey_numbers {
                        if let Some(pos) = vehicle_positions.get(sub) {
                            messages.push(Message::Text(
                                serde_json::to_string(&WsResponse::VehiclePosition(pos.clone()))
                                    .unwrap(),
                            ));
                        }
                    }
                }

                for msg in messages {
                    let _ = socket.send(dbg!(msg)).await;
                }

                let mut live_rx = state.live_tx.subscribe();

                loop {
                    let pos = live_rx.recv().await.unwrap();

                    if let Some(journey_number) = &pos.journey_number {
                        if journey_numbers.contains(journey_number) {
                            let _ = socket
                                .send(Message::Text(
                                    serde_json::to_string(&WsResponse::VehiclePosition(dbg!(pos)))
                                        .unwrap(),
                                ))
                                .await;
                        }
                    }
                }
            }
        }
    }
}
