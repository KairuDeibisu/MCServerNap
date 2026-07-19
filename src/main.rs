use anyhow::{Context, Result, anyhow, bail};
use clap::{Parser, Subcommand};
use std::future::pending;
use std::io::ErrorKind;
use std::net::SocketAddr;
use std::path::PathBuf;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;
use tokio::time::{Duration, MissedTickBehavior, interval, timeout};

use mcservernap::config;
use mcservernap::msmp::{
    MsmpConfig, ServerStatus, SessionCommand, SessionUpdate, run_session, send_stop_command,
};
use mcservernap::{
    ServerState, launch_server, send_reconnecting_message, send_starting_message,
    verify_handshake_packet,
};

#[derive(Parser)]
#[command(name = "mcservernap")]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Own the Minecraft port while the server naps and launch it on the first login
    Listen {
        /// Host/IP to bind while Minecraft is stopped
        host: String,
        /// Shared Minecraft port owned by MCServerNap while the server is stopped
        port: u16,
        /// Command to launch (for example java, run.bat, or a script path)
        cmd: String,
        /// Arguments for the server command
        #[arg(num_args(0..), trailing_var_arg = true, allow_hyphen_values = true)]
        args: Vec<String>,
        /// Deprecated compatibility option; when supplied it must equal PORT
        #[arg(long)]
        server_port: Option<u16>,
        /// Path to the Minecraft 26.2 server.properties file
        #[arg(long, default_value = "server.properties")]
        server_properties: PathBuf,
    },
    /// Immediately stop the Minecraft server through MSMP
    Stop {
        /// Path to the Minecraft 26.2 server.properties file
        #[arg(long, default_value = "server.properties")]
        server_properties: PathBuf,
    },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum SessionOrigin {
    StartupProbe,
    PortOccupiedProbe,
    Launched,
}

struct ManagementSession {
    updates: mpsc::Receiver<SessionUpdate>,
    commands: mpsc::Sender<SessionCommand>,
    task: JoinHandle<()>,
}

enum SessionMessage {
    Update(SessionUpdate),
    Closed,
}

enum ProcessEvent {
    Exited { success: bool, description: String },
    WaitFailed(String),
}

#[tokio::main]
async fn main() -> Result<()> {
    env_logger::Builder::from_default_env()
        .filter_level(log::LevelFilter::Info)
        .init();

    match Cli::parse().command {
        Commands::Listen {
            host,
            port,
            cmd,
            args,
            server_port,
            server_properties,
        } => {
            if let Some(server_port) = server_port {
                if server_port != port {
                    bail!(
                        "shared-port mode requires --server-port ({server_port}) to equal PORT ({port}); remove --server-port or use the same value"
                    );
                }
                log::warn!(
                    "--server-port is no longer needed; MCServerNap now uses shared-port mode"
                );
            }
            let address: SocketAddr = format!("{host}:{port}").parse()?;
            run_listen(address, cmd, args, server_properties).await?;
        }
        Commands::Stop { server_properties } => {
            send_stop_command(server_properties).await?;
        }
    }

    Ok(())
}

async fn run_listen(
    address: SocketAddr,
    command: String,
    args: Vec<String>,
    server_properties: PathBuf,
) -> Result<()> {
    let app_config = config::get_config();
    let management_config = MsmpConfig::from_server_properties(&server_properties)?;
    if management_config.server_port() != address.port() {
        bail!(
            "shared port {address} does not match server-port={} in {}; use the same port for MCServerNap and Minecraft",
            management_config.server_port(),
            server_properties.display()
        );
    }
    let idle_timeout = Duration::from_secs(app_config.idle_timeout);
    let command_args: Vec<&str> = args.iter().map(String::as_str).collect();

    log::info!(
        "Checking {} for an existing Minecraft server (up to 60 seconds)...",
        management_config.endpoint()
    );
    let mut session = Some(spawn_management_session(
        management_config.clone(),
        idle_timeout,
    ));
    let mut session_origin = Some(SessionOrigin::StartupProbe);
    let mut state = loop {
        let initial = tokio::select! {
            update = receive_session_update(&mut session) => update,
            _ = tokio::signal::ctrl_c() => {
                abort_session(&mut session);
                return Ok(());
            }
        };
        match initial {
            SessionMessage::Update(SessionUpdate::Snapshot(status)) => {
                log_snapshot(&status, "Initial MSMP snapshot");
                break state_from_status(&status);
            }
            SessionMessage::Update(SessionUpdate::Unavailable(error)) => {
                log::info!(
                    "No existing MSMP server was found after 60 seconds ({error}); treating Minecraft as stopped"
                );
                abort_session(&mut session);
                session_origin = None;
                break ServerState::Stopped;
            }
            SessionMessage::Closed => {
                abort_session(&mut session);
                session_origin = None;
                break ServerState::Stopped;
            }
            SessionMessage::Update(update) => {
                log::debug!("Ignoring pre-snapshot MSMP update: {update:?}");
            }
        }
    };

    let mut listener: Option<TcpListener> = None;
    let mut bind_retry = interval(Duration::from_secs(3));
    bind_retry.set_missed_tick_behavior(MissedTickBehavior::Delay);
    let mut bind_failure_logged = false;
    let (process_updates, mut process_events) = mpsc::channel::<ProcessEvent>(4);
    let mut launched_server_started = false;

    loop {
        tokio::select! {
            _ = bind_retry.tick(), if listener.is_none() && can_own_port(state) => {
                match TcpListener::bind(address).await {
                    Ok(new_listener) => {
                        bind_failure_logged = false;
                        listener = Some(new_listener);
                        if session_origin == Some(SessionOrigin::PortOccupiedProbe) {
                            abort_session(&mut session);
                            session_origin = None;
                            state = ServerState::Stopped;
                        }
                        match state {
                            ServerState::Stopped => log::info!("Minecraft is napping; listening on {address}"),
                            ServerState::Unknown => log::warn!("MSMP is reconnecting; temporarily listening on {address}"),
                            ServerState::Starting | ServerState::Running => {}
                        }
                    }
                    Err(error) if error.kind() == ErrorKind::AddrInUse => {
                        if !bind_failure_logged {
                            log::info!(
                                "Cannot listen on {address} because Minecraft still owns the shared port; retrying every 3 seconds"
                            );
                            bind_failure_logged = true;
                        }
                        if session.is_none() && matches!(state, ServerState::Stopped) {
                            log::info!("The shared port is occupied; attempting to attach to Minecraft through MSMP");
                            state = ServerState::Unknown;
                            session = Some(spawn_management_session(
                                management_config.clone(),
                                idle_timeout,
                            ));
                            session_origin = Some(SessionOrigin::PortOccupiedProbe);
                        }
                    }
                    Err(error) => {
                        return Err(error).with_context(|| format!("failed to listen on {address}"));
                    }
                }
            }
            accepted = accept_connection(&listener), if listener.is_some() => {
                let (mut client, peer) = match accepted {
                    Ok(connection) => connection,
                    Err(error) => {
                        log::warn!("Failed to accept Minecraft connection: {error}");
                        continue;
                    }
                };
                client.set_nodelay(true)?;
                log::info!("Incoming Minecraft connection from {peer}");

                match verify_handshake_packet(&mut client, peer, &app_config).await {
                    Ok(true) => match state {
                        ServerState::Stopped => {
                            send_starting_message(client, &app_config).await?;
                            listener.take();
                            state = ServerState::Starting;
                            launched_server_started = false;
                            log::info!("Released {address}; launching Minecraft on the shared port");

                            let mut child = launch_server(&command, &command_args)?;
                            let process_updates = process_updates.clone();
                            tokio::spawn(async move {
                                let event = match child.wait().await {
                                    Ok(status) => ProcessEvent::Exited {
                                        success: status.success(),
                                        description: status.to_string(),
                                    },
                                    Err(error) => ProcessEvent::WaitFailed(error.to_string()),
                                };
                                let _ = process_updates.send(event).await;
                            });

                            abort_session(&mut session);
                            session = Some(spawn_management_session(
                                management_config.clone(),
                                idle_timeout,
                            ));
                            session_origin = Some(SessionOrigin::Launched);
                        }
                        ServerState::Unknown => {
                            send_reconnecting_message(client, &app_config).await?;
                        }
                        ServerState::Starting => {
                            send_starting_message(client, &app_config).await?;
                        }
                        ServerState::Running => {
                            log::debug!("Dropping stale placeholder connection from {peer}");
                        }
                    },
                    Ok(false) => {}
                    Err(error) => log::warn!("Failed to handle Minecraft connection from {peer}: {error}"),
                }
            }
            message = receive_session_update(&mut session) => {
                match message {
                    SessionMessage::Update(SessionUpdate::Snapshot(status)) => {
                        state = state_from_status(&status);
                        if session_origin == Some(SessionOrigin::Launched) && status.started {
                            launched_server_started = true;
                        }
                        log_snapshot(&status, "MSMP state update");
                        if !can_own_port(state) {
                            listener.take();
                        }
                    }
                    SessionMessage::Update(SessionUpdate::ServerStarted) => {
                        state = ServerState::Running;
                        if session_origin == Some(SessionOrigin::Launched) {
                            launched_server_started = true;
                        }
                        listener.take();
                        log::info!("Minecraft started; MSMP event monitoring is active");
                    }
                    SessionMessage::Update(SessionUpdate::ServerStopping) => {
                        log::info!("Minecraft is stopping; preparing to reclaim {address}");
                        state = ServerState::Stopped;
                        abort_session(&mut session);
                        session_origin = None;
                    }
                    SessionMessage::Update(SessionUpdate::PlayerCount(count)) => {
                        log::info!("MSMP event reports {count} connected player(s)");
                    }
                    SessionMessage::Update(SessionUpdate::ConnectionInterrupted(error)) => {
                        state = ServerState::Unknown;
                        log::warn!("MSMP connection lost ({error}); state is unknown while reconnecting");
                    }
                    SessionMessage::Update(SessionUpdate::ReconnectAttempt(attempt)) => {
                        log::warn!("MSMP reconnect attempt {attempt}/3 in 3 seconds");
                    }
                    SessionMessage::Update(SessionUpdate::ConnectionLost(error)) => {
                        if session_origin == Some(SessionOrigin::Launched) && !launched_server_started {
                            abort_session(&mut session);
                            return Err(anyhow!("Minecraft failed to establish a stable MSMP session: {error}"));
                        }
                        log::error!("{error}; treating Minecraft as stopped");
                        state = ServerState::Stopped;
                        abort_session(&mut session);
                        session_origin = None;
                    }
                    SessionMessage::Update(SessionUpdate::Unavailable(error)) => {
                        let origin = session_origin.take();
                        abort_session(&mut session);
                        match origin {
                            Some(SessionOrigin::Launched) => {
                                return Err(anyhow!("Minecraft failed to start: {error}"));
                            }
                            Some(SessionOrigin::StartupProbe | SessionOrigin::PortOccupiedProbe) | None => {
                                log::warn!("{error}; treating Minecraft as stopped");
                                state = ServerState::Stopped;
                            }
                        }
                    }
                    SessionMessage::Closed => {
                        if !matches!(state, ServerState::Stopped) {
                            log::warn!("MSMP session ended without a final state; treating state as unknown");
                            state = ServerState::Unknown;
                        }
                        abort_session(&mut session);
                        session_origin = None;
                    }
                }
            }
            process = process_events.recv() => {
                let Some(process) = process else {
                    continue;
                };
                match process {
                    ProcessEvent::Exited { success, description } => {
                        log::info!("Minecraft process exited ({description})");
                        if session_origin == Some(SessionOrigin::Launched) && !launched_server_started {
                            abort_session(&mut session);
                            return Err(anyhow!(
                                "Minecraft exited before MSMP reported it started (successful exit: {success})"
                            ));
                        }
                        state = ServerState::Stopped;
                        abort_session(&mut session);
                        session_origin = None;
                    }
                    ProcessEvent::WaitFailed(error) => {
                        log::error!("Failed to wait for Minecraft process: {error}");
                    }
                }
            }
            _ = tokio::signal::ctrl_c() => {
                log::info!("Shutdown signal received");
                if let Some(active_session) = session.as_ref() {
                    if let Err(error) = request_stop(active_session).await {
                        log::error!("Failed to stop Minecraft through the persistent MSMP session: {error}");
                    }
                }
                abort_session(&mut session);
                return Ok(());
            }
        }
    }
}

fn spawn_management_session(config: MsmpConfig, idle_timeout: Duration) -> ManagementSession {
    let (updates_tx, updates) = mpsc::channel(32);
    let (commands, commands_rx) = mpsc::channel(4);
    let task = tokio::spawn(run_session(config, idle_timeout, updates_tx, commands_rx));
    ManagementSession {
        updates,
        commands,
        task,
    }
}

fn abort_session(session: &mut Option<ManagementSession>) {
    if let Some(session) = session.take() {
        session.task.abort();
    }
}

async fn request_stop(session: &ManagementSession) -> Result<()> {
    let (reply, response) = oneshot::channel();
    session
        .commands
        .send(SessionCommand::Stop { reply })
        .await
        .context("persistent MSMP session is not available")?;
    timeout(Duration::from_secs(12), response)
        .await
        .context("timed out waiting for MSMP stop response")?
        .context("MSMP session ended before replying")?
        .map_err(anyhow::Error::msg)
}

async fn receive_session_update(session: &mut Option<ManagementSession>) -> SessionMessage {
    match session {
        Some(session) => match session.updates.recv().await {
            Some(update) => SessionMessage::Update(update),
            None => SessionMessage::Closed,
        },
        None => pending::<SessionMessage>().await,
    }
}

async fn accept_connection(
    listener: &Option<TcpListener>,
) -> std::io::Result<(TcpStream, SocketAddr)> {
    match listener {
        Some(listener) => listener.accept().await,
        None => pending::<std::io::Result<(TcpStream, SocketAddr)>>().await,
    }
}

fn can_own_port(state: ServerState) -> bool {
    matches!(state, ServerState::Unknown | ServerState::Stopped)
}

fn state_from_status(status: &ServerStatus) -> ServerState {
    if status.started {
        ServerState::Running
    } else {
        ServerState::Starting
    }
}

fn log_snapshot(status: &ServerStatus, context: &str) {
    log::info!(
        "{context}: started={}, players={}",
        status.started,
        status.players.len()
    );
}
