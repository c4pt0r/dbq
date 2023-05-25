# dbq
a small distributed queue using database as backend


## Features

- Create a new message queue.
- Delete an existing message queue.
- Push a new message to a queue.
- Pull messages from a queue.
- Retrieve a specific message from a queue.
- Update a message in a queue.
- Clear a queue (delete all messages).

## Installation

   ```
   go build cmd/httpserver.go 
   ```

## Configuration

The program uses a configuration file in TOML format to specify the TiDB connection and server settings. By default, the program looks for a file named `config.toml` in the current directory. You can specify a different configuration file using the `-c` command-line flag.


Go to tidb.cloud to get a free database cluster

The default configuration file (`config.toml`) has the following structure:

```toml
[tidb]
username = "root"
password = ""
host = ""
port = 4000
database = "dbq"
tls_enabled = true

[tidb.tls_config]
server_name = ""

[server]
port = 8080
host = "localhost"
debug = true
```

- `tidb`: TiDB database configuration.
  - `username`: TiDB username.
  - `password`: TiDB password.
  - `host`: TiDB host.
  - `port`: TiDB port.
  - `database`: TiDB database name.
  - `tls_enabled`: Enable TLS connection to TiDB (true/false).
  - `tls_config`: TLS configuration.
    - `server_name`: Server name for TLS handshake.

- `server`: API server configuration.
  - `port`: API server port.
  - `host`: API server host.
  - `debug`: Enable debug mode (true/false).

## Usage

1. Start the program:

   ```
   ./httpserver -c config.toml
   ```

   or

   ```
   AUTH_TOKEN=<your-auth-token> ./httpserver -c config.toml
   ```

   This will start the API server using the configuration specified in the `config.toml` file.

2. The API server will listen for incoming requests at the configured host and port (e.g., `localhost:8080`).

3. You can interact with the message queue API using any HTTP client or tool, such as cURL or Postman.

4. Refer to the API endpoints below for the available operations:

   - `POST /q/:name`: Create a new message queue with the specified name.
   - `DELETE /q/:name`: Delete an existing message queue with the specified name.
   - `POST /q/:name/push`: Push a new message to the queue with the specified name.
   - `GET /q/:name/pull?limit=n`: Pull up to `n` messages from the queue with the specified name.
   - `GET /q/:name/msg/:id`: Retrieve the message with the specified ID from the queue with the specified name.
   - `PUT /q/:name/msg/:id`: Update the message with the specified ID in the queue with the specified name.
   - `DELETE /q/:name/truncate`: Clear (delete all messages) from the queue with the specified name.

## Example


1. Create a new message queue:
   ```bash
   http POST http://localhost:8080/q/myqueue
   ```

2. Delete an existing message queue:
   ```bash
   http DELETE http://localhost:8080/q/myqueue
   ```

3. Push a new message to a queue:
   ```bash
   echo '{"message": "Hello, World!"}' | http POST http://localhost:8080/q/myqueue/push
   ```

4. Pull messages from a queue:
   ```bash
   http GET http://localhost:8080/q/myqueue/pull?limit=10
   ```

5. Retrieve a specific message from a queue:
   ```bash
   http GET http://localhost:8080/q/myqueue/msg/12345
   ```

6. Update a message in a queue:
   ```bash
   echo '{"data": "Updated message"}' | http PUT http://localhost:8080/q/myqueue/msg/12345
   ```

7. Clear a queue (delete all messages):
   ```bash
   http DELETE http://localhost:8080/q/myqueue/truncate
   ```
