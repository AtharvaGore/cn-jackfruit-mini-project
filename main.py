#!/usr/bin/env python3
"""Socket-based publish-subscribe system in pure Python.

Protocol
--------
All messages are newline-delimited JSON objects.

Client -> Broker messages:
  {"type": "subscribe", "topic": "sports"}
  {"type": "unsubscribe", "topic": "sports"}
  {"type": "publish", "topic": "sports", "data": "Team A won!"}

Broker -> Client messages:
  {"type": "ack", "message": "..."}
  {"type": "event", "topic": "sports", "data": "Team A won!", "timestamp": 1711111111.1}
  {"type": "error", "message": "..."}

Usage examples:
  python main.py broker --host 127.0.0.1 --port 5555
  python main.py subscriber --host 127.0.0.1 --port 5555 --topics sports tech
  python main.py publisher --host 127.0.0.1 --port 5555
"""

from __future__ import annotations

import argparse
import json
import socket
import ssl
import threading
import time
from dataclasses import dataclass, field
from typing import Dict, Optional, Set


def send_json(conn: socket.socket, payload: dict, send_lock: Optional[threading.Lock] = None) -> None:
	"""Send one JSON message terminated with a newline."""
	data = (json.dumps(payload, separators=(",", ":")) + "\n").encode("utf-8")
	if send_lock is None:
		conn.sendall(data)
		return
	with send_lock:
		conn.sendall(data)


@dataclass
class ClientState:
	conn: socket.socket
	address: tuple[str, int]
	subscriptions: Set[str] = field(default_factory=set)
	send_lock: threading.Lock = field(default_factory=threading.Lock)


class BrokerServer:
	def __init__(
		self,
		host: str,
		port: int,
		use_ssl: bool = False,
		certfile: Optional[str] = None,
		keyfile: Optional[str] = None,
		cafile: Optional[str] = None,
		require_client_cert: bool = False,
	) -> None:
		self.host = host
		self.port = port
		self.use_ssl = use_ssl
		self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
		self.server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

		self._ssl_context: Optional[ssl.SSLContext] = None
		if self.use_ssl:
			if not certfile or not keyfile:
				raise ValueError("SSL requires --certfile and --keyfile for broker mode")
			ctx = ssl.create_default_context(ssl.Purpose.CLIENT_AUTH)
			ctx.load_cert_chain(certfile=certfile, keyfile=keyfile)
			if cafile:
				ctx.load_verify_locations(cafile=cafile)
			ctx.verify_mode = ssl.CERT_REQUIRED if require_client_cert else ssl.CERT_NONE
			self._ssl_context = ctx

		self._lock = threading.Lock()
		self._clients: Dict[socket.socket, ClientState] = {}
		self._topic_subscribers: Dict[str, Set[socket.socket]] = {}

	# this function is the broker's entry point. it starts the server and begins listening on the given port.

	def start(self) -> None:
		self.server_socket.bind((self.host, self.port))
		self.server_socket.listen()
		transport = "TLS" if self.use_ssl else "TCP"
		print(f"[BROKER] Transport: {transport}")
		print(f"[BROKER] Listening on {self.host}:{self.port}")

		while True:
			# takes in a message, creates a state for the client and adds it to the clients dict
			raw_conn, addr = self.server_socket.accept()
			conn = raw_conn
			if self.use_ssl and self._ssl_context is not None:
				try:
					conn = self._ssl_context.wrap_socket(raw_conn, server_side=True)
				except ssl.SSLError as exc:
					print(f"[BROKER] TLS handshake failed for {addr}: {exc}")
					raw_conn.close()
					continue
			state = ClientState(conn=conn, address=addr)
			with self._lock:
				self._clients[conn] = state
			print(f"[BROKER] Client connected: {addr}")

			# creates a thread to handle the new client request
			threading.Thread(target=self._handle_client, args=(state,), daemon=True).start()

	def _handle_client(self, state: ClientState) -> None:

		# this function takes the message sent by the client and then sends it further to the process message function
		conn = state.conn
		try:
			with conn, conn.makefile("r", encoding="utf-8") as reader:
				for line in reader:
					line = line.strip()
					if not line:
						continue
					self._process_message(state, line)
		except Exception as exc:
			print(f"[BROKER] Connection error {state.address}: {exc}")
		finally:
			self._remove_client(state)
			print(f"[BROKER] Client disconnected: {state.address}")

	def _process_message(self, state: ClientState, raw: str) -> None:

		# here the broker checks if the sender is a subscriber or a broker, then also validates the json.

		try:
			msg = json.loads(raw)
		except json.JSONDecodeError:
			send_json(state.conn, {"type": "error", "message": "Invalid JSON"}, state.send_lock)
			return

		msg_type = msg.get("type")
		topic = msg.get("topic")

		if msg_type == "subscribe":
			if not self._valid_topic(topic):
				send_json(state.conn, {"type": "error", "message": "Missing/invalid topic"}, state.send_lock)
				return
			self._subscribe(state, topic)
			send_json(state.conn, {"type": "ack", "message": f"Subscribed to '{topic}'"}, state.send_lock)
			return

		if msg_type == "unsubscribe":
			if not self._valid_topic(topic):
				send_json(state.conn, {"type": "error", "message": "Missing/invalid topic"}, state.send_lock)
				return
			self._unsubscribe(state, topic)
			send_json(state.conn, {"type": "ack", "message": f"Unsubscribed from '{topic}'"}, state.send_lock)
			return

		if msg_type == "publish":
			if not self._valid_topic(topic):
				send_json(state.conn, {"type": "error", "message": "Missing/invalid topic"}, state.send_lock)
				return
			event = {
				"type": "event",
				"topic": topic,
				"data": msg.get("data"),
				"timestamp": time.time(),
			}
			delivered = self._publish(topic, event)
			send_json(
				state.conn,
				{"type": "ack", "message": f"Published to '{topic}', delivered to {delivered} subscriber(s)"},
				state.send_lock,
			)
			return

		send_json(state.conn, {"type": "error", "message": f"Unknown message type: {msg_type}"}, state.send_lock)

	@staticmethod
	def _valid_topic(topic: object) -> bool:
		return isinstance(topic, str) and bool(topic.strip())

	def _subscribe(self, state: ClientState, topic: str) -> None:
		with self._lock:
			state.subscriptions.add(topic)
			self._topic_subscribers.setdefault(topic, set()).add(state.conn)

	def _unsubscribe(self, state: ClientState, topic: str) -> None:
		with self._lock:
			state.subscriptions.discard(topic)
			subscribers = self._topic_subscribers.get(topic)
			if subscribers is not None:
				subscribers.discard(state.conn)
				if not subscribers:
					del self._topic_subscribers[topic]

	def _publish(self, topic: str, event: dict) -> int:

		# this function publishes the message to all the subscribers

		with self._lock:
			recipients = list(self._topic_subscribers.get(topic, set()))

		delivered = 0
		dead: list[socket.socket] = []
		for conn in recipients:
			state = self._clients.get(conn)
			if state is None:
				dead.append(conn)
				continue
			try:
				send_json(conn, event, state.send_lock)
				delivered += 1
			except OSError:
				dead.append(conn)

		for conn in dead:
			state = self._clients.get(conn)
			if state is not None:
				self._remove_client(state)
		return delivered

	def _remove_client(self, state: ClientState) -> None:
		with self._lock:
			if state.conn not in self._clients:
				return
			del self._clients[state.conn]

			for topic in list(state.subscriptions):
				subscribers = self._topic_subscribers.get(topic)
				if subscribers is None:
					continue
				subscribers.discard(state.conn)
				if not subscribers:
					del self._topic_subscribers[topic]


def run_subscriber(
	host: str,
	port: int,
	initial_topics: list[str],
	use_ssl: bool = False,
	cafile: Optional[str] = None,
	certfile: Optional[str] = None,
	keyfile: Optional[str] = None,
	server_hostname: Optional[str] = None,
	insecure: bool = False,
) -> None:
	conn = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
	if use_ssl:
		ctx = ssl.create_default_context(ssl.Purpose.SERVER_AUTH)
		if insecure:
			ctx.check_hostname = False
			ctx.verify_mode = ssl.CERT_NONE
		elif cafile:
			ctx.load_verify_locations(cafile=cafile)
		if certfile:
			ctx.load_cert_chain(certfile=certfile, keyfile=keyfile)
		hostname = server_hostname if server_hostname else host
		conn = ctx.wrap_socket(conn, server_hostname=hostname)
	conn.connect((host, port))
	print(f"[SUBSCRIBER] Connected to broker {host}:{port}")

	for topic in initial_topics:
		send_json(conn, {"type": "subscribe", "topic": topic})

	stop_event = threading.Event()

	def recv_loop() -> None:
		try:
			with conn.makefile("r", encoding="utf-8") as reader:
				for line in reader:
					line = line.strip()
					if not line:
						continue
					try:
						msg = json.loads(line)
					except json.JSONDecodeError:
						print("[SUBSCRIBER] Received invalid JSON")
						continue

					if msg.get("type") == "event":
						ts = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(msg.get("timestamp", time.time())))
						print(f"[EVENT {ts}] {msg.get('topic')}: {msg.get('data')}")
					else:
						print(f"[BROKER] {msg}")
		except Exception as exc:
			if not stop_event.is_set():
				print(f"[SUBSCRIBER] Receive error: {exc}")
		finally:
			stop_event.set()

	threading.Thread(target=recv_loop, daemon=True).start()

	print("[SUBSCRIBER] Commands: sub <topic>, unsub <topic>, quit")
	try:
		while not stop_event.is_set():
			command = input("> ").strip()
			if not command:
				continue
			if command == "quit":
				break
			if command.startswith("sub "):
				topic = command[4:].strip()
				if topic:
					send_json(conn, {"type": "subscribe", "topic": topic})
				continue
			if command.startswith("unsub "):
				topic = command[6:].strip()
				if topic:
					send_json(conn, {"type": "unsubscribe", "topic": topic})
				continue
			print("[SUBSCRIBER] Unknown command")
	except KeyboardInterrupt:
		pass
	finally:
		stop_event.set()
		conn.close()


def run_publisher(
	host: str,
	port: int,
	topic: Optional[str],
	message: Optional[str],
	use_ssl: bool = False,
	cafile: Optional[str] = None,
	certfile: Optional[str] = None,
	keyfile: Optional[str] = None,
	server_hostname: Optional[str] = None,
	insecure: bool = False,
) -> None:
	# this function runs when a publisher sends a message
	

	# opens a tcp connection to the broker. the host and port are passed as args

	conn = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
	if use_ssl:
		ctx = ssl.create_default_context(ssl.Purpose.SERVER_AUTH)
		if insecure:
			ctx.check_hostname = False
			ctx.verify_mode = ssl.CERT_NONE
		elif cafile:
			ctx.load_verify_locations(cafile=cafile)
		if certfile:
			ctx.load_cert_chain(certfile=certfile, keyfile=keyfile)
		hostname = server_hostname if server_hostname else host
		conn = ctx.wrap_socket(conn, server_hostname=hostname)
	conn.connect((host, port))
	reader = conn.makefile("r", encoding="utf-8")
	print(f"[PUBLISHER] Connected to broker {host}:{port}")


	# this function actually sends the data to the broker. once it is sent, the publisher terminates.

	def publish_once(out_topic: str, out_message: str) -> None:
		send_json(conn, {"type": "publish", "topic": out_topic, "data": out_message})
		line = reader.readline().strip()
		if line:
			print(f"[BROKER] {line}")


	# does input validation then sends the message to the broker via the connection socket
	try:
		if topic is not None and message is not None:
			publish_once(topic, message)
			return


	# all this is just the publisher making sure the input the user gives is correct
		print("[PUBLISHER] Enter messages as: <topic> <message>. Type 'quit' to exit.")
		while True:
			raw = input("> ").strip()
			if raw == "quit":
				break
			if not raw:
				continue
			parts = raw.split(maxsplit=1)
			if len(parts) < 2:
				print("[PUBLISHER] Format: <topic> <message>")
				continue
			out_topic, out_message = parts
			send_json(conn, {"type": "publish", "topic": out_topic, "data": out_message})
			line = reader.readline().strip()
			if line:
				print(f"[BROKER] {line}")
	except KeyboardInterrupt:
		pass

	# termination code
	finally:
		reader.close()
		conn.close()


def build_parser() -> argparse.ArgumentParser:
	parser = argparse.ArgumentParser(description="Socket-based publish-subscribe system")
	subparsers = parser.add_subparsers(dest="mode", required=True)

	broker_parser = subparsers.add_parser("broker", help="Run broker server")
	broker_parser.add_argument("--host", default="127.0.0.1")
	broker_parser.add_argument("--port", type=int, default=5555)
	broker_parser.add_argument("--ssl", action="store_true", help="Enable TLS on broker socket")
	broker_parser.add_argument("--certfile", help="Path to server certificate PEM file")
	broker_parser.add_argument("--keyfile", help="Path to server private key PEM file")
	broker_parser.add_argument("--cafile", help="CA bundle for verifying client certificates")
	broker_parser.add_argument("--require-client-cert", action="store_true", help="Require client certificate")

	subscriber_parser = subparsers.add_parser("subscriber", help="Run subscriber client")
	subscriber_parser.add_argument("--host", default="127.0.0.1")
	subscriber_parser.add_argument("--port", type=int, default=5555)
	subscriber_parser.add_argument("--topics", nargs="*", default=[])
	subscriber_parser.add_argument("--ssl", action="store_true", help="Enable TLS for client connection")
	subscriber_parser.add_argument("--cafile", help="CA bundle to verify server certificate")
	subscriber_parser.add_argument("--certfile", help="Client certificate PEM file")
	subscriber_parser.add_argument("--keyfile", help="Client private key PEM file")
	subscriber_parser.add_argument("--server-hostname", help="TLS server hostname override")
	subscriber_parser.add_argument("--insecure", action="store_true", help="Disable certificate verification (dev only)")

	publisher_parser = subparsers.add_parser("publisher", help="Run publisher client")
	publisher_parser.add_argument("--host", default="127.0.0.1")
	publisher_parser.add_argument("--port", type=int, default=5555)
	publisher_parser.add_argument("--topic")
	publisher_parser.add_argument("--message")
	publisher_parser.add_argument("--ssl", action="store_true", help="Enable TLS for client connection")
	publisher_parser.add_argument("--cafile", help="CA bundle to verify server certificate")
	publisher_parser.add_argument("--certfile", help="Client certificate PEM file")
	publisher_parser.add_argument("--keyfile", help="Client private key PEM file")
	publisher_parser.add_argument("--server-hostname", help="TLS server hostname override")
	publisher_parser.add_argument("--insecure", action="store_true", help="Disable certificate verification (dev only)")

	return parser


def main() -> None:
	# this is the program entry point. based on the arguments it runs a different type of client
	args = build_parser().parse_args()


	if args.mode == "broker":
		BrokerServer(
			args.host,
			args.port,
			use_ssl=args.ssl,
			certfile=args.certfile,
			keyfile=args.keyfile,
			cafile=args.cafile,
			require_client_cert=args.require_client_cert,
		).start()
		return

	if args.mode == "subscriber":
		run_subscriber(
			args.host,
			args.port,
			args.topics,
			use_ssl=args.ssl,
			cafile=args.cafile,
			certfile=args.certfile,
			keyfile=args.keyfile,
			server_hostname=args.server_hostname,
			insecure=args.insecure,
		)
		return

	if args.mode == "publisher":
		run_publisher(
			args.host,
			args.port,
			args.topic,
			args.message,
			use_ssl=args.ssl,
			cafile=args.cafile,
			certfile=args.certfile,
			keyfile=args.keyfile,
			server_hostname=args.server_hostname,
			insecure=args.insecure,
		)
		return

	raise ValueError(f"Unsupported mode: {args.mode}")


if __name__ == "__main__":
	main()
