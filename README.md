# MCServerNap

*A lightweight, serverless Minecraft server watcher and auto-starter.*

## Overview

`mcservernap` monitors incoming Minecraft client connections and automatically launches and stops a local Minecraft 26.2 server through the Minecraft Server Management Protocol (MSMP) 3.0.0. It enables you to avoid running your server 24/7 by:

* Listening for the first legitimate Minecraft **LoginStart** handshake.
* Spinning up the server process on-demand when a player attempts to join.
* Holding the Minecraft port while the server sleeps, then releasing it before launch.
* Watching one persistent MSMP connection for player and lifecycle events.
* Stopping the server after an idle timeout.

<img width="657" height="94" alt="screenshot of server browser view" src="https://github.com/user-attachments/assets/dae15e22-849e-4469-bae9-df17cc94636b" />
<img width="966" height="261" alt="screenshot of connect message" src="https://github.com/user-attachments/assets/ca128f11-5e7a-4666-a03c-6d56235385db" />


There is also a `stop` subcommand that immediately requests a graceful shutdown through MSMP.

## Features

* **On-demand startup**: server only runs when a player actually joins.
* **Idle shutdown**: automatically stops server when no players remain for a set duration.
* **Cross-platform**: spawns a new terminal window on Windows, runs directly on Linux systems.
* **Shared-port takeover**: MCServerNap and Minecraft use the same public port at different times; no traffic proxy is required.
* **Event-driven management**: uses one persistent authenticated MSMP WebSocket instead of polling.
* **Existing-server attachment**: detects an already-running server at startup and enforces the same idle timeout.

## Installation

1. Ensure you have Rust and Cargo installed (see [rustup.rs](https://rustup.rs)).
2. Clone this repository:

   ```bash
   git clone https://github.com/yourusername/MCServerNap.git
   cd MCServerNap
   ```
3. Build the binary:

   ```bash
   cargo build --release
   ```

   The executable can be found under `target/release/mcservernap.exe`
4. (Optional) If you wish to install globally:

   ```bash
   cargo install --path .
   ```

## Usage

```bash
mcservernap <COMMAND> [OPTIONS]
```

### Subcommands

* `listen` — Listen for incoming connections and start the server on first join.
* `stop` — Immediately stop an already-running server through MSMP.

### `listen` Options

| Option                | Description                                                          | Required |
| --------------------- | -------------------------------------------------------------------- | -------- |
| `host`                | Host or IP to bind (e.g. `0.0.0.0`)                                  | Yes      |
| `port`                | Port to listen on for Minecraft clients                              | Yes      |
| `cmd`                 | Command or script to launch the Minecraft server                     | Yes      |
| `args...`             | Arguments passed to the server command                               | No       |
| `--server-properties` | Path to the Minecraft 26.2 `server.properties` file                  | No       |

The positional `port` must be the same port configured as `server-port` in Minecraft's `server.properties`. The older `--server-port` option is accepted only for command-line compatibility and must match `port` when supplied.

> [!IMPORTANT]
> When not using a script and instead executing a command with its own arguments, you need to append the command to the end of the line followed by `--` and all the arguments of the command. See below for an example!

> [!NOTE]
> The port of the Minecraft server does not require port forwarding, only the port of this application.

#### Example

```bash
mcservernap listen 0.0.0.0 25565 --server-properties ./server.properties java -- -Xmx5G -Xms5G -jar server.jar nogui
```

#### Script Example

```bash
mcservernap listen 0.0.0.0 25565 --server-properties "C:\path\to\your\server.properties" "C:\path\to\your\script\run.bat"
```
**IMPORTANT: When using a script, make sure the script closes its window at the end of the script (Windows .bat example: `exit`), or else this application won't detect that the Minecraft server process has shut down!**

Once a client sends a LoginStart packet, the tool:

1. Checks for an existing MSMP server every 3 seconds for up to 60 seconds. If found, it takes one status snapshot and attaches to it.
2. If Minecraft is stopped, binds the shared Minecraft port and answers status pings.
3. On a real login, releases the port, launches the server command, and waits up to 60 seconds for Minecraft to start.
4. Maintains one MSMP WebSocket and listens for server-started, server-stopping, player-joined, player-left, and status-heartbeat notifications.
5. If no players remain for `idle_timeout`, requests a graceful stop through that same connection.
6. When Minecraft stops, reclaims the shared port and begins listening again.

If MSMP disconnects, MCServerNap marks state unknown and retries three times at three-second intervals. If it can reclaim the shared port during that window, login attempts receive the configurable reconnecting message. After three failed reconnects, it treats Minecraft as stopped and continues trying to bind the shared port every three seconds.

### `stop` Options

| Option                | Description                                         | Required |
| --------------------- | --------------------------------------------------- | -------- |
| `--server-properties` | Path to the Minecraft 26.2 `server.properties` file | No       |

#### Example

```bash
mcservernap stop --server-properties ./server.properties
```

This immediately connects to the management server and invokes `minecraft:server/stop`.

## Configuration & Environment

### Minecraft 26.2 management server

MSMP must use a fixed local port and plaintext WebSockets. Configure these values in the same `server.properties` file passed to MCServerNap:

```properties
management-server-enabled=true
management-server-host=127.0.0.1
management-server-port=25585
management-server-secret=0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcd
management-server-tls-enabled=false
server-port=25565
```

Replace the example secret with your own 40-character alphanumeric value. MCServerNap only accepts a loopback management host because plaintext MSMP carries the bearer secret without transport encryption. Port `0` is not supported because MCServerNap needs a stable endpoint.

### Logging

Controlled via the entry point of `main()`:

```rust
env_logger::Builder::from_default_env()
        .filter_level(log::LevelFilter::Info) // Change this LevelFilter to change logging level (e.g. Debug)
        .init();
```
You need to rebuild the project for the change to take effect.

### The **configuration** will be generated on first time usage of this application under `config/cfg.toml`
Configuration Options:
* **Idle timeout**: set via `idle_timeout` in <ins>seconds</ins>. Player join/leave notifications drive the timer; there is no polling interval.
* **Message of the day (MOTD)**: The message shown to the user in the server browser menu. set via `motd_text`, `motd_color` and `motd_bold`
* **Connection Message**: The message shown to the user when they try to connect. Set via `connection_msg_text`, `connection_msg_color` and `connection_msg_bold`
* **Reconnecting Message**: The message shown while MSMP state is unknown and MCServerNap owns the shared port. Set via `reconnecting_msg_text`, `reconnecting_msg_color` and `reconnecting_msg_bold`
* **Server Icon**: The icon of the server within the server browser menu. Set by inserting a `.png` file in the `config/` folder with the name `server-icon.png`. The image must be 64x64 pixels big. If it's not, this application will automatically resize the image to meet this requirement
* **Configuration Directory**: The location of the `cfg.toml` can be changed from the standard `config/` directory by editing the value of `config_directory_name`. This will delete the previous directory and move the files to the new one

## Contributing

Contributions are welcome. Feel free to open issues or pull requests.

## License

This project is licensed under the MIT License. See [LICENSE](LICENSE) for details.
