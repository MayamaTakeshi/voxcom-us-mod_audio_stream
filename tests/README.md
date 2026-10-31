# Outgoing backpressure tests

These tests build the actual libwsc sources without loading or restarting
FreeSWITCH. They require CMake, a C++ compiler, the normal libwsc development
dependencies, Python 3, and the OpenSSL command-line tool. They generate a
temporary self-signed certificate and listen only on loopback.

```sh
cmake -S tests -B build/backpressure-tests -DCMAKE_BUILD_TYPE=Debug -DENABLE_SANITIZERS=ON
cmake --build build/backpressure-tests -j2
python3 tests/test_backpressure.py build/backpressure-tests/backpressure_client
```

Run outside ptrace-based sandboxes so LeakSanitizer can inspect the child
processes. Omit `-DENABLE_SANITIZERS=ON` if sanitizer runtimes are unavailable.
The tests use separate processes and never connect to the running FreeSWITCH.

Both ws and wss cover:

- normal binary and text traffic received by an echo server;
- a producer filling the pending queue while the event thread is busy, including
  mixed binary/text accounting and rejection after exactly 1 MiB;
- rejection of a single oversized message before it is copied into the queue;
- a peer that accepts the handshake but stops reading, with sends from the
  open callback to deterministically fill the libevent output buffer before the
  socket can drain. Fifteen 64 KiB frames fit; the next exceeds the 1 MiB cap.

A further case negotiates permessage-deflate and sends incompressible data that
fits the raw input limit but exceeds the wire limit after compression.

Overflow cases require one error callback, send rejection and successful cleanup.
The output-buffer case exercises direct sends from the event thread, which bypass
the application queue. Normal traffic must complete without an error callback.

Additional review regressions:

```sh
python3 tests/test_command_payload.py
build/backpressure-tests/receiver_limits
```

The receiver executable checks oversized 64-bit frame lengths, aggregate
fragment limits, and decompression expansion. The network suite also rejects
invalid handshake accept values over both ws and wss. Command parsing tests
compile the actual module parser block and check spaces, apostrophes, and JSON
escapes. Live-call validation is still needed for playback and teardown timing.
