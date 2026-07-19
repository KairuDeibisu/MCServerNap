use anyhow::{Context, Result, anyhow, bail};
use futures_util::{SinkExt, StreamExt};
use serde::Deserialize;
use serde_json::{Value, json};
use std::collections::{HashMap, VecDeque};
use std::fs;
use std::future::pending;
use std::path::Path;
use tokio::net::TcpStream;
use tokio::sync::{mpsc, oneshot};
use tokio::time::{Duration, Instant, sleep, sleep_until, timeout};
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::http::header::{AUTHORIZATION, HeaderValue};
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream, connect_async};

const JSON_RPC_VERSION: &str = "2.0";
const SERVER_STATUS_METHOD: &str = "minecraft:server/status";
const SERVER_STOP_METHOD: &str = "minecraft:server/stop";
const NOTIFICATION_PREFIX: &str = "minecraft:notification/";
const INITIAL_CONNECT_WINDOW: Duration = Duration::from_secs(60);
const RETRY_INTERVAL: Duration = Duration::from_secs(3);
const RECONNECT_ATTEMPTS: u8 = 3;

#[derive(Clone)]
pub struct MsmpConfig {
    url: String,
    secret: String,
    server_port: u16,
}

impl MsmpConfig {
    pub fn from_server_properties(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref();
        let contents = fs::read_to_string(path)
            .with_context(|| format!("failed to read {}", path.display()))?;
        let properties = parse_server_properties(&contents);

        require_property(&properties, "management-server-enabled", "true")?;
        require_property(&properties, "management-server-tls-enabled", "false")?;

        let host = properties
            .get("management-server-host")
            .map(String::as_str)
            .filter(|host| !host.is_empty())
            .unwrap_or("localhost");
        let port = required_property(&properties, "management-server-port")?
            .parse::<u16>()
            .context("management-server-port must be a valid TCP port")?;
        if port == 0 {
            bail!("management-server-port must be fixed and nonzero so MCServerNap can connect");
        }

        let secret = required_property(&properties, "management-server-secret")?.to_owned();
        if secret.len() != 40
            || !secret
                .chars()
                .all(|character| character.is_ascii_alphanumeric())
        {
            bail!(
                "management-server-secret must contain exactly 40 ASCII letters or digits; start Minecraft once so it can generate one, or configure one explicitly"
            );
        }

        if !matches!(host, "localhost" | "127.0.0.1" | "::1" | "[::1]") {
            bail!(
                "management-server-host must be localhost, 127.0.0.1, or ::1 when using plaintext MSMP"
            );
        }

        let host = if host.contains(':') && !host.starts_with('[') {
            format!("[{host}]")
        } else {
            host.to_owned()
        };

        let server_port = properties
            .get("server-port")
            .map(String::as_str)
            .unwrap_or("25565")
            .parse::<u16>()
            .context("server-port must be a valid TCP port")?;

        Ok(Self {
            url: format!("ws://{host}:{port}"),
            secret,
            server_port,
        })
    }

    pub fn endpoint(&self) -> &str {
        &self.url
    }

    pub fn server_port(&self) -> u16 {
        self.server_port
    }
}

fn required_property<'a>(properties: &'a HashMap<String, String>, key: &str) -> Result<&'a str> {
    properties
        .get(key)
        .map(String::as_str)
        .ok_or_else(|| anyhow!("missing required server property {key}"))
}

fn require_property(properties: &HashMap<String, String>, key: &str, expected: &str) -> Result<()> {
    let actual = required_property(properties, key)?;
    if !actual.eq_ignore_ascii_case(expected) {
        bail!("{key} must be {expected}, but is {actual}");
    }
    Ok(())
}

fn parse_server_properties(contents: &str) -> HashMap<String, String> {
    contents
        .lines()
        .filter_map(|line| {
            let line = line.trim();
            if line.is_empty() || line.starts_with('#') || line.starts_with('!') {
                return None;
            }

            let (key, value) = line.split_once('=')?;
            Some((key.trim().to_owned(), value.trim().to_owned()))
        })
        .collect()
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
pub struct ServerStatus {
    pub started: bool,
    #[serde(default)]
    pub players: Vec<Player>,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
pub struct Player {
    #[serde(default)]
    pub id: Option<String>,
    #[serde(default)]
    pub name: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum MsmpEvent {
    ServerStarted,
    ServerStopping,
    PlayerJoined(Player),
    PlayerLeft(Player),
    Status(ServerStatus),
    Other(String),
}

pub struct MsmpClient {
    socket: WebSocketStream<MaybeTlsStream<TcpStream>>,
    next_request_id: u64,
    pending_events: VecDeque<MsmpEvent>,
}

impl MsmpClient {
    pub async fn connect(config: &MsmpConfig) -> Result<Self> {
        let mut request = config
            .url
            .as_str()
            .into_client_request()
            .with_context(|| format!("invalid management server URL {}", config.url))?;
        let bearer = HeaderValue::from_str(&format!("Bearer {}", config.secret))
            .context("management-server-secret is not valid as an HTTP header value")?;
        request.headers_mut().insert(AUTHORIZATION, bearer);

        let (socket, _) = timeout(Duration::from_secs(5), connect_async(request))
            .await
            .context("timed out connecting to MSMP")?
            .with_context(|| format!("failed to connect to MSMP at {}", config.url))?;

        Ok(Self {
            socket,
            next_request_id: 1,
            pending_events: VecDeque::new(),
        })
    }

    pub async fn server_status(&mut self) -> Result<ServerStatus> {
        let value = self.call(SERVER_STATUS_METHOD).await?;
        serde_json::from_value(value).context("invalid minecraft:server/status response")
    }

    pub async fn stop(&mut self) -> Result<()> {
        self.call(SERVER_STOP_METHOD).await?;
        Ok(())
    }

    pub async fn next_event(&mut self) -> Result<MsmpEvent> {
        if let Some(event) = self.pending_events.pop_front() {
            return Ok(event);
        }

        while let Some(message) = self.socket.next().await {
            match message.context("failed to read MSMP notification")? {
                Message::Text(text) => {
                    let message: Value = serde_json::from_str(text.as_str())
                        .context("MSMP returned invalid JSON")?;
                    if let Some(event) = parse_notification_or_warn(&message) {
                        return Ok(event);
                    }
                }
                Message::Ping(payload) => {
                    self.socket.send(Message::Pong(payload)).await?;
                }
                Message::Close(frame) => {
                    bail!("MSMP connection closed: {frame:?}");
                }
                Message::Binary(_) | Message::Pong(_) | Message::Frame(_) => {}
            }
        }

        bail!("MSMP connection ended")
    }

    async fn call(&mut self, method: &str) -> Result<Value> {
        timeout(Duration::from_secs(5), self.call_inner(method))
            .await
            .with_context(|| format!("MSMP method {method} timed out"))?
    }

    async fn call_inner(&mut self, method: &str) -> Result<Value> {
        let id = self.next_request_id;
        self.next_request_id += 1;
        let request = json!({
            "jsonrpc": JSON_RPC_VERSION,
            "id": id,
            "method": method,
        });

        self.socket
            .send(Message::Text(request.to_string().into()))
            .await
            .with_context(|| format!("failed to send MSMP method {method}"))?;

        while let Some(message) = self.socket.next().await {
            match message.context("failed to read MSMP response")? {
                Message::Text(text) => {
                    let response: Value = serde_json::from_str(text.as_str())
                        .context("MSMP returned invalid JSON")?;

                    if response.get("id").and_then(Value::as_u64) == Some(id) {
                        if let Some(error) = response.get("error") {
                            bail!("MSMP method {method} failed: {error}");
                        }
                        return response
                            .get("result")
                            .cloned()
                            .ok_or_else(|| anyhow!("MSMP method {method} returned no result"));
                    }

                    if let Some(event) = parse_notification_or_warn(&response) {
                        self.pending_events.push_back(event);
                    }
                }
                Message::Ping(payload) => {
                    self.socket.send(Message::Pong(payload)).await?;
                }
                Message::Close(frame) => {
                    bail!("MSMP connection closed while waiting for {method}: {frame:?}");
                }
                Message::Binary(_) | Message::Pong(_) | Message::Frame(_) => {}
            }
        }

        bail!("MSMP connection ended while waiting for {method}")
    }
}

fn parse_notification(message: &Value) -> Result<Option<MsmpEvent>> {
    if message.get("id").is_some() {
        return Ok(None);
    }

    let Some(method) = message.get("method").and_then(Value::as_str) else {
        return Ok(None);
    };
    let path = method.strip_prefix(NOTIFICATION_PREFIX).unwrap_or(method);
    let params = message.get("params").cloned().unwrap_or(Value::Null);

    let event = match path {
        "server/started" => MsmpEvent::ServerStarted,
        "server/stopping" => MsmpEvent::ServerStopping,
        "players/joined" => MsmpEvent::PlayerJoined(decode_params(params, "player")?),
        "players/left" => MsmpEvent::PlayerLeft(decode_params(params, "player")?),
        "server/status" => MsmpEvent::Status(decode_params(params, "status")?),
        _ => MsmpEvent::Other(method.to_owned()),
    };
    Ok(Some(event))
}

fn parse_notification_or_warn(message: &Value) -> Option<MsmpEvent> {
    match parse_notification(message) {
        Ok(event) => event,
        Err(error) => {
            log::warn!("Ignoring invalid MSMP notification: {error:#}");
            None
        }
    }
}

fn decode_params<T: for<'de> Deserialize<'de>>(params: Value, parameter: &str) -> Result<T> {
    let value = match params {
        Value::Array(mut values) => {
            if values.len() != 1 {
                bail!(
                    "invalid MSMP notification parameter {parameter}: expected one positional value, got {}",
                    values.len()
                );
            }
            values.pop().expect("parameter count was checked")
        }
        Value::Object(mut values) => match values.remove(parameter) {
            Some(value) => value,
            None => Value::Object(values),
        },
        value => value,
    };

    serde_json::from_value(value)
        .with_context(|| format!("invalid MSMP notification parameter {parameter}"))
}

#[derive(Debug)]
pub enum SessionUpdate {
    Snapshot(ServerStatus),
    ServerStarted,
    ServerStopping,
    PlayerCount(usize),
    ConnectionInterrupted(String),
    ReconnectAttempt(u8),
    ConnectionLost(String),
    Unavailable(String),
}

pub enum SessionCommand {
    Stop {
        reply: oneshot::Sender<std::result::Result<(), String>>,
    },
}

struct TrackedServer {
    started: bool,
    players: Vec<Player>,
    idle_deadline: Option<Instant>,
    startup_deadline: Option<Instant>,
    idle_timeout: Duration,
}

impl TrackedServer {
    fn new(status: &ServerStatus, idle_timeout: Duration) -> Self {
        let now = Instant::now();
        Self {
            started: status.started,
            players: status.players.clone(),
            idle_deadline: (status.started && status.players.is_empty())
                .then_some(now + idle_timeout),
            startup_deadline: (!status.started).then_some(now + INITIAL_CONNECT_WINDOW),
            idle_timeout,
        }
    }

    fn apply_snapshot(&mut self, status: &ServerStatus) {
        let was_started = self.started;
        let had_players = !self.players.is_empty();
        self.started = status.started;
        self.players.clone_from(&status.players);

        if self.started {
            self.startup_deadline = None;
            if self.players.is_empty() {
                if !was_started || had_players || self.idle_deadline.is_none() {
                    self.idle_deadline = Some(Instant::now() + self.idle_timeout);
                }
            } else {
                self.idle_deadline = None;
            }
        } else {
            self.idle_deadline = None;
            if self.startup_deadline.is_none() {
                self.startup_deadline = Some(Instant::now() + INITIAL_CONNECT_WINDOW);
            }
        }
    }

    fn server_started(&mut self) {
        self.started = true;
        self.startup_deadline = None;
        if self.players.is_empty() {
            self.idle_deadline = Some(Instant::now() + self.idle_timeout);
        }
    }

    fn player_joined(&mut self, player: Player) {
        if !self
            .players
            .iter()
            .any(|current| same_player(current, &player))
        {
            self.players.push(player);
        }
        self.idle_deadline = None;
    }

    fn player_left(&mut self, player: &Player) {
        let previously_online = !self.players.is_empty();
        self.players.retain(|current| !same_player(current, player));
        if self.started && previously_online && self.players.is_empty() {
            self.idle_deadline = Some(Instant::now() + self.idle_timeout);
        }
    }
}

fn same_player(left: &Player, right: &Player) -> bool {
    left.id
        .as_ref()
        .zip(right.id.as_ref())
        .is_some_and(|(left, right)| left == right)
        || left
            .name
            .as_ref()
            .zip(right.name.as_ref())
            .is_some_and(|(left, right)| left == right)
}

pub async fn run_session(
    config: MsmpConfig,
    idle_timeout: Duration,
    updates: mpsc::Sender<SessionUpdate>,
    mut commands: mpsc::Receiver<SessionCommand>,
) {
    let (mut client, initial_status) = match connect_with_initial_retry(&config).await {
        Ok(connection) => connection,
        Err(error) => {
            let _ = updates
                .send(SessionUpdate::Unavailable(error.to_string()))
                .await;
            return;
        }
    };

    log::info!("Connected to persistent MSMP session at {}", config.url);
    let mut tracked = TrackedServer::new(&initial_status, idle_timeout);
    if updates
        .send(SessionUpdate::Snapshot(initial_status))
        .await
        .is_err()
    {
        return;
    }

    loop {
        match run_connected(&mut client, &mut tracked, &updates, &mut commands).await {
            ConnectedOutcome::Finished => return,
            ConnectedOutcome::Disconnected(error) => {
                if updates
                    .send(SessionUpdate::ConnectionInterrupted(error.to_string()))
                    .await
                    .is_err()
                {
                    return;
                }

                match reconnect(&config, &updates).await {
                    Ok((new_client, status)) => {
                        client = new_client;
                        tracked.apply_snapshot(&status);
                        if updates.send(SessionUpdate::Snapshot(status)).await.is_err() {
                            return;
                        }
                    }
                    Err(error) => {
                        let _ = updates
                            .send(SessionUpdate::ConnectionLost(error.to_string()))
                            .await;
                        return;
                    }
                }
            }
        }
    }
}

enum ConnectedOutcome {
    Finished,
    Disconnected(anyhow::Error),
}

async fn run_connected(
    client: &mut MsmpClient,
    tracked: &mut TrackedServer,
    updates: &mpsc::Sender<SessionUpdate>,
    commands: &mut mpsc::Receiver<SessionCommand>,
) -> ConnectedOutcome {
    loop {
        tokio::select! {
            event = client.next_event() => {
                let event = match event {
                    Ok(event) => event,
                    Err(error) => return ConnectedOutcome::Disconnected(error),
                };
                match event {
                    MsmpEvent::ServerStarted => {
                        tracked.server_started();
                        if updates.send(SessionUpdate::ServerStarted).await.is_err() {
                            return ConnectedOutcome::Finished;
                        }
                    }
                    MsmpEvent::ServerStopping => {
                        let _ = updates.send(SessionUpdate::ServerStopping).await;
                        return ConnectedOutcome::Finished;
                    }
                    MsmpEvent::PlayerJoined(player) => {
                        tracked.player_joined(player);
                        if updates.send(SessionUpdate::PlayerCount(tracked.players.len())).await.is_err() {
                            return ConnectedOutcome::Finished;
                        }
                    }
                    MsmpEvent::PlayerLeft(player) => {
                        tracked.player_left(&player);
                        if updates.send(SessionUpdate::PlayerCount(tracked.players.len())).await.is_err() {
                            return ConnectedOutcome::Finished;
                        }
                    }
                    MsmpEvent::Status(status) => {
                        tracked.apply_snapshot(&status);
                        if updates.send(SessionUpdate::Snapshot(status)).await.is_err() {
                            return ConnectedOutcome::Finished;
                        }
                    }
                    MsmpEvent::Other(method) => {
                        log::debug!("Ignoring MSMP notification {method}");
                    }
                }
            }
            command = commands.recv() => {
                let Some(command) = command else {
                    return ConnectedOutcome::Finished;
                };
                match command {
                    SessionCommand::Stop { reply } => {
                        let result = client.stop().await.map_err(|error| error.to_string());
                        let failed = result.is_err();
                        let error = result.as_ref().err().cloned();
                        let _ = reply.send(result);
                        if failed {
                            return ConnectedOutcome::Disconnected(anyhow!(
                                "failed to send stop request: {}",
                                error.unwrap_or_else(|| "unknown error".to_owned())
                            ));
                        }
                    }
                }
            }
            _ = wait_for_deadline(tracked.idle_deadline) => {
                log::info!("No players for {:?}; requesting server stop through MSMP", tracked.idle_timeout);
                tracked.idle_deadline = None;
                if let Err(error) = client.stop().await {
                    return ConnectedOutcome::Disconnected(error.context("idle stop request failed"));
                }
            }
            _ = wait_for_deadline(tracked.startup_deadline) => {
                let _ = updates.send(SessionUpdate::Unavailable(
                    "MSMP connected, but Minecraft did not report started within 60 seconds".to_owned()
                )).await;
                return ConnectedOutcome::Finished;
            }
        }
    }
}

async fn wait_for_deadline(deadline: Option<Instant>) {
    match deadline {
        Some(deadline) => sleep_until(deadline).await,
        None => pending::<()>().await,
    }
}

async fn connect_and_snapshot(config: &MsmpConfig) -> Result<(MsmpClient, ServerStatus)> {
    let mut client = MsmpClient::connect(config).await?;
    let status = client.server_status().await?;
    Ok((client, status))
}

async fn connect_with_initial_retry(config: &MsmpConfig) -> Result<(MsmpClient, ServerStatus)> {
    let deadline = Instant::now() + INITIAL_CONNECT_WINDOW;
    let last_error = loop {
        match connect_and_snapshot(config).await {
            Ok(connection) => return Ok(connection),
            Err(error) => {
                let now = Instant::now();
                if now >= deadline {
                    break error;
                }
                sleep(RETRY_INTERVAL.min(deadline - now)).await;
            }
        }
    };

    Err(last_error).context("MSMP was unavailable for 60 seconds")
}

async fn reconnect(
    config: &MsmpConfig,
    updates: &mpsc::Sender<SessionUpdate>,
) -> Result<(MsmpClient, ServerStatus)> {
    let mut last_error = None;
    for attempt in 1..=RECONNECT_ATTEMPTS {
        if updates
            .send(SessionUpdate::ReconnectAttempt(attempt))
            .await
            .is_err()
        {
            bail!("application stopped while reconnecting to MSMP");
        }
        sleep(RETRY_INTERVAL).await;
        match connect_and_snapshot(config).await {
            Ok(connection) => return Ok(connection),
            Err(error) => last_error = Some(error),
        }
    }

    Err(last_error.unwrap_or_else(|| anyhow!("MSMP reconnect failed")))
        .context("MSMP reconnect failed after 3 attempts")
}

pub async fn send_stop_command(properties_path: impl AsRef<Path>) -> Result<()> {
    let config = MsmpConfig::from_server_properties(properties_path)?;
    log::info!("Connecting to MSMP at {} to stop the server...", config.url);
    let mut client = MsmpClient::connect(&config).await?;
    client.stop().await?;
    log::info!("Stop request sent.");
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::net::TcpListener;
    use tokio_tungstenite::accept_hdr_async;
    use tokio_tungstenite::tungstenite::handshake::server::{Request, Response};

    #[test]
    fn parses_plaintext_management_configuration() {
        let properties = parse_server_properties(
            r#"
                # Minecraft server properties
                management-server-enabled=true
                management-server-host=127.0.0.1
                management-server-port=25585
                management-server-secret=abcdefghijklmnopqrstuvwxyz1234567890ABCD
                management-server-tls-enabled=false
            "#,
        );

        assert_eq!(
            properties.get("management-server-port").map(String::as_str),
            Some("25585")
        );
        assert_eq!(
            properties
                .get("management-server-tls-enabled")
                .map(String::as_str),
            Some("false")
        );
    }

    #[test]
    fn parses_server_status() {
        let status: ServerStatus = serde_json::from_value(json!({
            "started": true,
            "players": [
                {"id": "853c80ef-3c37-49fd-aa49-938b674adae6", "name": "jeb_"}
            ],
            "version": {"name": "26.2", "protocol": 776}
        }))
        .unwrap();

        assert!(status.started);
        assert_eq!(status.players.len(), 1);
        assert_eq!(status.players[0].name.as_deref(), Some("jeb_"));
    }

    #[test]
    fn parses_official_notification_names_and_payloads() {
        let joined = parse_notification(&json!({
            "jsonrpc": "2.0",
            "method": "minecraft:notification/players/joined",
            "params": [{"id": "player-id", "name": "Alex"}]
        }))
        .unwrap();
        assert_eq!(
            joined,
            Some(MsmpEvent::PlayerJoined(Player {
                id: Some("player-id".to_owned()),
                name: Some("Alex".to_owned()),
            }))
        );

        let status = parse_notification(&json!({
            "jsonrpc": "2.0",
            "method": "minecraft:notification/server/status",
            "params": [{"started": true, "players": []}]
        }))
        .unwrap();
        assert_eq!(
            status,
            Some(MsmpEvent::Status(ServerStatus {
                started: true,
                players: vec![],
            }))
        );
    }

    #[test]
    fn event_tracking_preserves_idle_deadline_across_empty_heartbeats() {
        let status = ServerStatus {
            started: true,
            players: vec![],
        };
        let mut tracked = TrackedServer::new(&status, Duration::from_secs(600));
        let original_deadline = tracked.idle_deadline;

        tracked.apply_snapshot(&status);
        assert_eq!(tracked.idle_deadline, original_deadline);

        let player = Player {
            id: Some("player-id".to_owned()),
            name: Some("Alex".to_owned()),
        };
        tracked.player_joined(player.clone());
        assert_eq!(tracked.players.len(), 1);
        assert!(tracked.idle_deadline.is_none());

        tracked.player_left(&player);
        assert!(tracked.players.is_empty());
        assert!(tracked.idle_deadline.is_some());
    }

    #[tokio::test]
    async fn authenticates_queries_once_then_receives_notifications() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let secret = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcd";

        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let mut socket = accept_hdr_async(stream, |request: &Request, response: Response| {
                assert_eq!(
                    request.headers().get(AUTHORIZATION).unwrap(),
                    &format!("Bearer {secret}")
                );
                Ok(response)
            })
            .await
            .unwrap();

            let request = socket.next().await.unwrap().unwrap();
            let Message::Text(request) = request else {
                panic!("expected a text request");
            };
            let request: Value = serde_json::from_str(request.as_str()).unwrap();
            assert_eq!(request["method"], SERVER_STATUS_METHOD);
            assert_eq!(request["id"], 1);

            socket
                .send(Message::Text(
                    json!({
                        "jsonrpc": "2.0",
                        "method": "minecraft:notification/players/joined",
                        "params": [
                            {"id": "first-player", "name": "Alex"},
                            {"id": "second-player", "name": "Steve"}
                        ]
                    })
                    .to_string()
                    .into(),
                ))
                .await
                .unwrap();
            socket
                .send(Message::Text(
                    json!({
                        "jsonrpc": "2.0",
                        "method": "minecraft:notification/players/joined",
                        "params": [{"id": "player-id", "name": "Alex"}]
                    })
                    .to_string()
                    .into(),
                ))
                .await
                .unwrap();
            socket
                .send(Message::Text(
                    json!({
                        "jsonrpc": "2.0",
                        "id": 1,
                        "result": {"started": true, "players": []}
                    })
                    .to_string()
                    .into(),
                ))
                .await
                .unwrap();
        });

        let config = MsmpConfig {
            url: format!("ws://{address}"),
            secret: secret.to_owned(),
            server_port: 25565,
        };
        let mut client = MsmpClient::connect(&config).await.unwrap();
        let status = client.server_status().await.unwrap();
        assert!(status.started);
        assert!(status.players.is_empty());
        assert_eq!(
            client.next_event().await.unwrap(),
            MsmpEvent::PlayerJoined(Player {
                id: Some("player-id".to_owned()),
                name: Some("Alex".to_owned()),
            })
        );
        server.await.unwrap();
    }
}
