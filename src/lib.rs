pub mod config;
pub mod msmp;

use crate::config::Config;
use anyhow::{Result, bail};
use serde_json::{Value, json};
use std::io::ErrorKind;
use std::net::SocketAddr;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::time::{Duration, timeout};

const MAX_PACKET_LENGTH: usize = 2 * 1024 * 1024;

/// Basic enum to provide state machine system for server status
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ServerState {
    Unknown,
    Stopped,
    Starting,
    Running,
}

/// Read a VarInt (Minecraft format) from the buffer, returning (value, bytes_read). Returns None if malformed
fn read_varint(buf: &[u8]) -> Option<(i32, usize)> {
    let mut num_read = 0;
    let mut result = 0i32;
    for &byte in buf.iter() {
        let val = (byte & 0x7F) as i32;
        result |= val << (7 * num_read);
        num_read += 1;
        if byte & 0x80 == 0 {
            return Some((result, num_read));
        }
        if num_read >= 5 {
            return None;
        }
    }
    None
}

/// Read exactly one length-prefixed Minecraft packet without consuming bytes
/// belonging to the next packet on the TCP stream.
async fn read_packet(socket: &mut TcpStream) -> Result<Option<Vec<u8>>> {
    let mut packet_length = 0i32;

    for byte_index in 0..5 {
        let mut byte = [0u8; 1];
        let bytes_read = socket.read(&mut byte).await?;
        if bytes_read == 0 {
            if byte_index == 0 {
                return Ok(None);
            }
            bail!("connection closed in the middle of a packet length");
        }

        packet_length |= ((byte[0] & 0x7f) as i32) << (7 * byte_index);
        if byte[0] & 0x80 == 0 {
            if packet_length < 0 {
                bail!("negative Minecraft packet length");
            }

            let packet_length = packet_length as usize;
            if packet_length > MAX_PACKET_LENGTH {
                bail!("Minecraft packet is too large: {packet_length} bytes");
            }

            let mut packet = vec![0u8; packet_length];
            socket.read_exact(&mut packet).await?;
            return Ok(Some(packet));
        }
    }

    bail!("Minecraft packet length VarInt exceeds 5 bytes")
}

// Write a VarInt (Minecraft format)
pub fn write_varint(mut val: i32, buf: &mut Vec<u8>) {
    loop {
        if (val & !0x7F) == 0 {
            buf.push(val as u8);
            return;
        } else {
            buf.push(((val & 0x7F) | 0x80) as u8);
            val >>= 7;
        }
    }
}

// Verifies a full Minecraft handshake on a single TcpStream.
pub async fn verify_handshake_packet(
    socket: &mut TcpStream,
    peer: SocketAddr,
    config: &Config,
) -> Result<bool> {
    // Read only the framed handshake packet. A status request is commonly
    // coalesced into the same TCP segment and must remain available for the
    // status-state handler below.
    let packet = match timeout(Duration::from_secs(5), read_packet(socket)).await {
        Ok(Ok(None)) => {
            log::debug!("Connection closed immediately by {}", peer);
            return Ok(false);
        }
        Ok(Ok(Some(packet))) => packet,
        Ok(Err(e))
            if e.downcast_ref::<std::io::Error>()
                .is_some_and(|e| e.kind() == ErrorKind::ConnectionReset) =>
        {
            log::debug!("Connection reset by peer {} (ignoring)", peer);
            return Ok(false);
        }
        Ok(Err(e)) => {
            // Unexpected I/O error, propagate
            return Err(e.into());
        }
        Err(_) => {
            log::debug!("Timeout waiting for data from {}", peer);
            return Ok(false);
        }
    };

    log::debug!("Received handshake packet: {:02X?}", packet);

    // Parse handshake packet (packet ID = 0).
    // More information on the handshake packet structure: https://minecraft.wiki/w/Java_Edition_protocol/Packets#Handshaking
    let (pkt_id, mut offset) = match read_varint(&packet) {
        Some(v) => v,
        None => return Ok(false),
    };
    if pkt_id != 0 {
        // not a handshake packet
        return Ok(false);
    }

    // Skip protocol version VarInt
    let (_protocol_version, len) = match read_varint(&packet[offset..]) {
        Some(v) => v,
        None => return Ok(false),
    };
    offset += len;

    // Read address length and skip the address string
    let (addr_len, len) = match read_varint(&packet[offset..]) {
        Some(v) => v,
        None => return Ok(false),
    };
    if addr_len < 0 {
        return Ok(false);
    }
    offset += len;
    offset = match offset.checked_add(addr_len as usize) {
        Some(end) if end <= packet.len() => end,
        _ => return Ok(false),
    };

    // Skip the port (2 bytes)
    offset = match offset.checked_add(2) {
        Some(end) if end <= packet.len() => end,
        _ => return Ok(false),
    };

    // Read next_state (intent) VarInt
    if offset >= packet.len() {
        return Ok(false);
    }
    if let Some((next_state, _)) = read_varint(&packet[offset..]) {
        if next_state == 1 {
            // Status ping
            handle_status_ping(socket, &config).await?;
            return Ok(false);
        } else if next_state == 2 {
            // Login handshake
            log::info!("Login handshake detected from {}", peer);
            return Ok(true);
        } else {
            log::debug!("Unknown type of ping from {}, ignoring", peer);
        }
    }

    Ok(false)
}

/// Launches the Minecraft server process with given command.
/// On Windows, opens the batch/script in a new terminal window so logs stay visible
pub fn launch_server(command: &str, args: &[&str]) -> Result<tokio::process::Child> {
    #[cfg(target_os = "windows")]
    {
        let mut cmd = tokio::process::Command::new("cmd");
        cmd.args(&["/C", "start", "", "/WAIT", command]);
        for &arg in args {
            cmd.arg(arg);
        }
        let child = cmd.spawn()?;
        log::info!("Launched server in new window: {} {:?}", command, args);
        Ok(child)
    }
    #[cfg(not(target_os = "windows"))]
    {
        let child = tokio::process::Command::new(command).args(args).spawn()?;
        log::info!("Launched server: {} {:?}", command, args);
        Ok(child)
    }
}

pub async fn send_starting_message(mut socket: TcpStream, config: &Config) -> Result<()> {
    send_disconnect_message(
        &mut socket,
        &config.connection_msg_text,
        &config.connection_msg_color,
        config.connection_msg_bold,
    )
    .await
}

pub async fn send_reconnecting_message(mut socket: TcpStream, config: &Config) -> Result<()> {
    send_disconnect_message(
        &mut socket,
        &config.reconnecting_msg_text,
        &config.reconnecting_msg_color,
        config.reconnecting_msg_bold,
    )
    .await
}

async fn send_disconnect_message(
    socket: &mut TcpStream,
    text: &str,
    color: &str,
    bold: bool,
) -> Result<()> {
    let json_msg = json!({
        "text": text,
        "color": color,
        "bold": bold
    })
    .to_string();
    let mut packet_data = Vec::new();

    //Packet ID 0x00 (login disconnect)
    write_varint(0, &mut packet_data);

    write_varint(json_msg.len() as i32, &mut packet_data);
    packet_data.extend_from_slice(json_msg.as_bytes());

    let mut packet = Vec::new();
    write_varint(packet_data.len() as i32, &mut packet);
    packet.extend_from_slice(&packet_data);

    match timeout(Duration::from_secs(5), socket.write_all(&packet)).await {
        Ok(Ok(())) => (),
        Ok(Err(e)) => log::warn!("Sending starting message to client failed: {:?}", e),
        Err(_) => log::warn!("Sending starting message to client timed out"),
    }

    // Wait a short moment to let client consume data (required because otherwise client doesn't display json message)
    tokio::time::sleep(Duration::from_millis(50)).await;

    socket.shutdown().await?;
    Ok(())
}

async fn handle_status_ping(socket: &mut TcpStream, config: &Config) -> Result<()> {
    // Status request packet: ID 0x00 with no payload. Reading a complete frame
    // is important because the handshake and request may share one TCP segment.
    let request = timeout(Duration::from_secs(5), read_packet(socket))
        .await
        .map_err(|_| anyhow::anyhow!("timed out waiting for Minecraft status request"))??
        .ok_or_else(|| anyhow::anyhow!("client closed before sending a status request"))?;
    let (request_id, request_id_length) = read_varint(&request)
        .ok_or_else(|| anyhow::anyhow!("status request has an invalid packet ID"))?;
    if request_id != 0 || request_id_length != request.len() {
        bail!("invalid Minecraft status request packet");
    }

    // Create custom MOTD JSON
    // Minecraft 26.2 uses Java protocol version 776.
    let mut motd_json_obj = json!({
        "version": {
            "name": "MCServerNap (26.2)",
            "protocol": 776
        },
        "players": {
            "max": 0,
            "online": 0,
            "sample": []
        },
        "description": {
            "text": config.motd_text,
            "color": config.motd_color,
            "bold": config.motd_bold
        }
    });

    if let Some(server_icon_base64) = config.server_icon.as_ref() {
        if let Value::Object(ref mut map) = motd_json_obj {
            map.insert(
                "favicon".to_string(),
                Value::String(format!("data:image/png;base64,{}", server_icon_base64)),
            );
        }
    }

    let motd_json = motd_json_obj.to_string();

    // Create status response packet
    let mut data = Vec::new();
    // Packet ID = 0 (status response)
    write_varint(0, &mut data);
    write_varint(motd_json.len() as i32, &mut data);
    data.extend_from_slice(motd_json.as_bytes());

    let mut packet = Vec::new();
    write_varint(data.len() as i32, &mut packet);
    packet.extend_from_slice(&data);

    timeout(Duration::from_secs(5), socket.write_all(&packet))
        .await
        .map_err(|_| anyhow::anyhow!("timed out sending Minecraft status response"))??;

    // Ping request packet: ID 0x01 followed by an arbitrary 8-byte payload.
    // Echoing that payload in a pong completes the server-list ping protocol.
    let ping = timeout(Duration::from_secs(5), read_packet(socket))
        .await
        .map_err(|_| anyhow::anyhow!("timed out waiting for Minecraft ping request"))??
        .ok_or_else(|| anyhow::anyhow!("client closed before sending a ping request"))?;
    let (ping_id, ping_id_length) = read_varint(&ping)
        .ok_or_else(|| anyhow::anyhow!("ping request has an invalid packet ID"))?;
    if ping_id != 1 || ping.len() != ping_id_length + 8 {
        bail!("invalid Minecraft ping request packet");
    }

    let mut pong_data = Vec::with_capacity(9);
    write_varint(1, &mut pong_data);
    pong_data.extend_from_slice(&ping[ping_id_length..]);

    let mut pong_packet = Vec::with_capacity(pong_data.len() + 1);
    write_varint(pong_data.len() as i32, &mut pong_packet);
    pong_packet.extend_from_slice(&pong_data);

    timeout(Duration::from_secs(5), socket.write_all(&pong_packet))
        .await
        .map_err(|_| anyhow::anyhow!("timed out sending Minecraft pong response"))??;

    socket.shutdown().await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::net::TcpListener;

    fn frame_packet(data: &[u8]) -> Vec<u8> {
        let mut packet = Vec::new();
        write_varint(data.len() as i32, &mut packet);
        packet.extend_from_slice(data);
        packet
    }

    #[tokio::test]
    async fn status_ping_handles_coalesced_handshake_and_request_and_replies_with_pong() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();

        let server = tokio::spawn(async move {
            let (mut socket, peer) = listener.accept().await.unwrap();
            verify_handshake_packet(&mut socket, peer, &Config::default()).await
        });

        let mut client = TcpStream::connect(address).await.unwrap();

        let mut handshake_data = Vec::new();
        write_varint(0, &mut handshake_data);
        write_varint(776, &mut handshake_data);
        write_varint(9, &mut handshake_data);
        handshake_data.extend_from_slice(b"localhost");
        handshake_data.extend_from_slice(&25565u16.to_be_bytes());
        write_varint(1, &mut handshake_data);

        let mut initial_write = frame_packet(&handshake_data);
        initial_write.extend_from_slice(&frame_packet(&[0]));
        client.write_all(&initial_write).await.unwrap();

        let response = read_packet(&mut client).await.unwrap().unwrap();
        let (response_id, response_id_length) = read_varint(&response).unwrap();
        assert_eq!(response_id, 0);
        let (json_length, json_length_size) = read_varint(&response[response_id_length..]).unwrap();
        let json_start = response_id_length + json_length_size;
        let status: Value = serde_json::from_slice(&response[json_start..]).unwrap();
        assert_eq!(json_length as usize, response.len() - json_start);
        assert_eq!(status["version"]["protocol"], 776);

        let ping_payload = 1_721_234_567_890i64.to_be_bytes();
        let mut ping_data = vec![1];
        ping_data.extend_from_slice(&ping_payload);
        client.write_all(&frame_packet(&ping_data)).await.unwrap();

        let pong = read_packet(&mut client).await.unwrap().unwrap();
        assert_eq!(pong[0], 1);
        assert_eq!(&pong[1..], &ping_payload);
        assert!(!server.await.unwrap().unwrap());
    }
}
