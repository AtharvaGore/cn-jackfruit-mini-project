# Socket Pub/Sub (Python)

A simple socket-based publish-subscribe system implemented in pure Python.

The project provides three modes:
- `broker`: accepts client connections and routes messages by topic
- `subscriber`: subscribes/unsubscribes to topics and receives events
- `publisher`: publishes messages to topics

## Requirements

- Python 3.10+ (or any recent Python 3 version)

## Project Files

- `main.py` - entry point for broker, subscriber, and publisher
- `server.crt` - TLS server certificate (optional TLS mode)
- `server.key` - TLS server private key (optional TLS mode)

## How To Run

Run all commands from the project root.

### 1) Start the broker (plain TCP)

```bash
python3 main.py broker --host 127.0.0.1 --port 5555
```

### 2) Start a subscriber (new terminal)

```bash
python3 main.py subscriber --host 127.0.0.1 --port 5555 --topics sports tech
```

Subscriber interactive commands:
- `sub <topic>`
- `unsub <topic>`
- `quit`

### 3) Publish a message

One-shot publish:

```bash
python3 main.py publisher --host 127.0.0.1 --port 5555 --topic sports --message "Team A won!"
```

Or interactive publisher mode:

```bash
python3 main.py publisher --host 127.0.0.1 --port 5555
```

Interactive input format:

```text
<topic> <message>
```

Type `quit` to exit.

## TLS (Optional)

You can enable TLS using the included certificate and key.

### Start broker with TLS

```bash
python3 main.py broker \
  --host 127.0.0.1 \
  --port 5555 \
  --ssl \
  --certfile server.crt \
  --keyfile server.key
```

### Connect subscriber with TLS (development mode)

```bash
python3 main.py subscriber --host 127.0.0.1 --port 5555 --ssl --insecure --topics sports
```

### Connect publisher with TLS (development mode)

```bash
python3 main.py publisher --host 127.0.0.1 --port 5555 --ssl --insecure --topic sports --message "hello over tls"
```

Note: `--insecure` disables certificate verification and is only recommended for local development.

## Help

For full CLI options:

```bash
python3 main.py --help
python3 main.py broker --help
python3 main.py subscriber --help
python3 main.py publisher --help
```
