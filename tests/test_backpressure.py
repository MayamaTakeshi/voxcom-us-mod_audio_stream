"""Exercise the real libwsc client against local ws/wss echo or stalled peers."""

import base64
import hashlib
import socket
import ssl
import struct
import subprocess
import sys
import tempfile
import threading
from pathlib import Path


def read_exact(conn, length):
    data = bytearray()
    while len(data) < length:
        chunk = conn.recv(length - len(data))
        if not chunk:
            raise EOFError
        data.extend(chunk)
    return bytes(data)


def serve_one(listener, context, mode):
    conn = None
    try:
        conn, _ = listener.accept()
        conn.settimeout(10)
        if context:
            conn = context.wrap_socket(conn, server_side=True)
        request = b""
        while not request.endswith(b"\r\n\r\n"):
            request += read_exact(conn, 1)
        headers = dict(line.split(b":", 1) for line in request.split(b"\r\n")[1:] if b":" in line)
        accept = base64.b64encode(hashlib.sha1(headers[b"Sec-WebSocket-Key"].strip()
                                             + b"258EAFA5-E914-47DA-95CA-C5AB0DC85B11").digest())
        extension = (b"Sec-WebSocket-Extensions: permessage-deflate; client_no_context_takeover; server_no_context_takeover\r\n"
                     if mode == "expansion" else b"")
        conn.sendall(b"HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\n"
                     b"Connection: Upgrade\r\nSec-WebSocket-Accept: " + accept + b"\r\n" + extension + b"\r\n")
        if mode != "normal":
            # No application reads after the handshake. The caller closes the
            # accepted socket by shutting the test server down after its client.
            stopped.wait(10)
            return
        while True:
            first, second = read_exact(conn, 2)
            size = second & 127
            if size == 126:
                size = struct.unpack("!H", read_exact(conn, 2))[0]
            elif size == 127:
                size = struct.unpack("!Q", read_exact(conn, 8))[0]
            mask = read_exact(conn, 4)
            payload = read_exact(conn, size)
            payload = bytes(byte ^ mask[i % 4] for i, byte in enumerate(payload))
            header = bytes([first, size]) if size < 126 else bytes([first, 126]) + struct.pack("!H", size)
            conn.sendall(header + payload)
            if first & 15 == 8:
                return
    except (OSError, EOFError):
        pass
    finally:
        if conn:
            conn.close()


if __name__ == "__main__":
    client = str(Path(sys.argv[1]).resolve())
    with tempfile.TemporaryDirectory() as directory:
        key, cert = Path(directory) / "key.pem", Path(directory) / "cert.pem"
        subprocess.run(["openssl", "req", "-x509", "-newkey", "rsa:2048", "-nodes",
                        "-keyout", str(key), "-out", str(cert), "-days", "1",
                        "-subj", "/CN=localhost"], check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        tls = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        tls.load_cert_chain(cert, key)
        for secure in (False, True):
            for mode in ("normal", "queue", "oversized", "output", "expansion"):
                stopped = threading.Event()
                with socket.socket() as listener:
                    listener.bind(("127.0.0.1", 0))
                    listener.listen(1)
                    server = threading.Thread(target=serve_one, args=(listener, tls if secure else None, mode))
                    server.start()
                    url = ("wss" if secure else "ws") + "://127.0.0.1:" + str(listener.getsockname()[1]) + "/"
                    try:
                        print(url, mode, flush=True)
                        subprocess.run([client, url, mode], check=True, timeout=15)
                    finally:
                        stopped.set()
                        server.join(timeout=12)
