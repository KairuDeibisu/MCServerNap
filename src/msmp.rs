use anyhow::{Context, Result, anyhow, bail};
use futures_util::{SinkExt, StreamExt};
use serde::Deserialize;
use serde_json::{Value, json};
use std::collections::HashMap;
use std::fs;
use std::path::Path;
use tokio::net::TcpStream;
use tokio::time::{Duration, timeout};
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::http::header::{AUTHORIZATION, HeaderValue};
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream, connect_async};

const JSON_RPC_VERSION: &str = "2.0";
const SERVER_STATUS_METHOD: &str = "minecraft:server/status";
const SERVER_STOP_METHOD: &str = "minecraft:server/stop";

#[derive(Clone)]
pub struct MsmpConfig {
    url: String,
    secret: String,
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

        Ok(Self {
            url: format!("ws://{host}:{port}"),
            secret,
        })
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

#[derive(Debug, Deserialize, PartialEq, Eq)]
pub struct ServerStatus {
    pub started: bool,
    #[serde(default)]
    pub players: Vec<Player>,
}

#[derive(Debug, Deserialize, PartialEq, Eq)]
pub struct Player {
    pub id: String,
    pub name: String,
}

pub struct MsmpClient {
    socket: WebSocketStream<MaybeTlsStream<TcpStream>>,
    next_request_id: u64,
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

                    if response.get("id").and_then(Value::as_u64) != Some(id) {
                        continue;
                    }
                    if let Some(error) = response.get("error") {
                        bail!("MSMP method {method} failed: {error}");
                    }
                    return response
                        .get("result")
                        .cloned()
                        .ok_or_else(|| anyhow!("MSMP method {method} returned no result"));
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
        assert_eq!(status.players[0].name, "jeb_");
    }

    #[tokio::test]
    async fn authenticates_and_calls_server_status() {
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
            assert_eq!(request["jsonrpc"], "2.0");
            assert_eq!(request["method"], SERVER_STATUS_METHOD);
            assert_eq!(request["id"], 1);

            socket
                .send(Message::Text(
                    json!({
                        "jsonrpc": "2.0",
                        "id": 1,
                        "result": {
                            "started": true,
                            "players": []
                        }
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
        };
        let mut client = MsmpClient::connect(&config).await.unwrap();
        let status = client.server_status().await.unwrap();

        assert!(status.started);
        assert!(status.players.is_empty());
        server.await.unwrap();
    }
}
