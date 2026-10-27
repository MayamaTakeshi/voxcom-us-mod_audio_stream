# mod_audio_stream

**Production-grade WebSocket audio streaming for FreeSWITCH.**

Streams real-time audio between FreeSWITCH and external systems with correct lifecycle management, thread safety and predictable memory usage.

***This module supports bi-directional audio streaming (see python example below).***

## About

- The purpose of `mod_audio_stream` was to provide a simple, low-dependency yet effective module for streaming audio and receiving responses from a websocket server.

## Installation

### Dependencies

It requires `libfreeswitch-dev`, `libssl-dev`, `zlib1g-dev`, `libevent-dev` and `libspeexdsp-dev` on Debian/Ubuntu which are regular packages for Freeswitch installation.

### Building

The WebSocket client library is included under `libs/libwsc`; no submodule initialization is required.

#### Custom path

If you built FreeSWITCH from source, eq. install dir is /usr/local/freeswitch, add path to pkgconfig:

```shell
export PKG_CONFIG_PATH=/usr/local/freeswitch/lib/pkgconfig
```

To build the module, from the cloned repository:

```shell
mkdir build && cd build
cmake -DCMAKE_BUILD_TYPE=Release ..
make
sudo make install
```

**TLS** is `OFF` by default. To build with TLS support add `-DUSE_TLS=ON` to cmake line.

#### DEB Package

To build DEB package after making the module:

```shell
cpack -G DEB
```

Debian package will be placed in root directory `_packages` folder.

## Scripted Build & Installation

```shell
sudo apt-get -y install git \
    && cd /usr/src/ \
    && git clone https://github.com/voxcom-us/mod_audio_stream.git \
    && cd mod_audio_stream \
    && sudo bash ./build-mod-audio-stream.sh
```

### Channel variables

The following channel variables can be used to fine tune websocket connection and also configure mod_audio_stream logging:

| Variable                               | Description                                             | Default |
| -------------------------------------- | ------------------------------------------------------- | ------- |
| STREAM_MESSAGE_DEFLATE                 | true or 1, disables per message deflate                 | off     |
| STREAM_HEART_BEAT                      | number of seconds, interval to send the heart beat      | off     |
| STREAM_SUPPRESS_LOG                    | true or 1, suppresses printing to log                   | off     |
| STREAM_BUFFER_SIZE                     | buffer duration in milliseconds, 20-2000 in 20ms steps  | 20      |
| STREAM_EXTRA_HEADERS                   | JSON object for additional headers in string format     | none    |
| ~~STREAM_NO_RECONNECT~~                    | true or 1, disables automatic websocket reconnection    | off     |
| STREAM_TLS_CA_FILE                     | CA cert or bundle, or the special values SYSTEM or NONE | SYSTEM  |
| STREAM_TLS_KEY_FILE                    | optional client key for WSS connections                 | none    |
| STREAM_TLS_CERT_FILE                   | optional client cert for WSS connections                | none    |
| STREAM_TLS_DISABLE_HOSTNAME_VALIDATION | true or 1 disable hostname check in WSS connections     | false   |

- Per message deflate compression option is enabled by default. It can lead to a very nice bandwidth savings. To disable it set the channel var to `true|1`.
- Heart beat, sent every xx seconds when there is no traffic to make sure that load balancers do not kill an idle connection.
- Suppress parameter is omitted by default(false). All the responses from websocket server will be printed to the log. Not to flood the log you can suppress it by setting the value to `true|1`. Events are fired still, it only affects printing to the log.
- `Buffer Size` represents the target duration of audio chunks sent to the WebSocket. Set it to 100 for approximately 100ms chunks. Valid values are 20-2000ms in 20ms increments; invalid values use 20ms. Codec frames that do not align with this target are buffered without dropping the overflow.
- Extra headers should be a JSON object with key-value pairs representing additional HTTP headers. Each key should be a header name, and its corresponding value should be a string.

  ```json
  {
      "Header1": "Value1",
      "Header2": "Value2",
      "Header3": "Value3"
  }
- ~~Websocket automatic reconnection is on by default. To disable it set this channel variable to true or 1.~~

  - libwsc does not support automatic reconnection. A dropped connection closes the audio stream; this setting has no effect.
- `flush` clears decoded audio queued for playback and discards stale in-flight audio for up to one second, so later audio is not suppressed indefinitely.
- TLS (for WSS) options can be fine tuned with the `STREAM_TLS_*` channel variables:
  - `STREAM_TLS_CA_FILE` the ca certificate (or certificate bundle) file. By default is `SYSTEM` which means use the system defaults.
Can be `NONE` which result in no peer verification.
  - `STREAM_TLS_CERT_FILE` optional client tls certificate file sent to the server.
  - `STREAM_TLS_KEY_FILE` optional client tls key file for the given certificate.
  - `STREAM_TLS_DISABLE_HOSTNAME_VALIDATION` if `true`, disables the check of the hostname against the peer server certificate.
Defaults to `false`, which enforces hostname match with the peer certificate.

## Outgoing buffering

Each connection limits pending application payloads to 1 MiB (including the
batch being serialized), and the libevent output buffer to 1 MiB including
WebSocket framing. The pending queue also retains its 1024-entry limit. A single
payload must fit these limits; the output check reserves up to 14 header bytes.
Checks apply to text, binary audio and control frames, before payload copying or
compression where possible, and again after compression to account for expansion.
These are payload/wire-buffer limits, not a total process-memory limit: socket
buffers, TLS, compression and serialization scratch space have their own overhead.

If either queue fills, libwsc rejects further sends and aborts the connection. The
module reports `mod_audio_stream::error` with code `1` (`IO`) and message
`Outgoing WebSocket buffer limit exceeded (1 MiB); peer is not keeping up`.
The existing error path removes the audio stream. libwsc does not automatically
reconnect. This prevents a slow or stalled peer from retaining unlimited caller
audio. It also makes overload visible instead of silently dropping arbitrary
pieces of audio or metadata. Successful enqueueing does not confirm delivery.

For isolated ws/wss backpressure regression tests, see [tests/README.md](tests/README.md).

## API

### Commands

The freeswitch module exposes the following API commands:

```shell
uuid_audio_stream <uuid> start <wss-url> <mix-type> <sampling-rate> <metadata>
```

Attaches a media bug and starts streaming audio (in L16 format) to the websocket server. FS default is 8k. If sampling-rate is other than 8k it will be resampled.

- `uuid` - Freeswitch channel unique id
- `wss-url` - websocket url `ws://` or `wss://`
- `mix-type` - choice of
  - "mono" - single channel containing caller's audio
  - "mixed" - single channel containing both caller and callee audio
  - "stereo" - two channels with caller audio in one and callee audio in the other.
- `sampling-rate` - choice of
  - "8k" = 8000 Hz sample rate will be generated
  - "16k" = 16000 Hz sample rate will be generated
- `metadata` - (optional) a valid `utf-8` text to send. It will be sent the first before audio streaming starts.

```shell
uuid_audio_stream <uuid> send_text <metadata>
```

Sends a text to the websocket server. Requires a valid `utf-8` text.

```shell
uuid_audio_stream <uuid> stop <metadata>
```

Stops audio stream and closes websocket connection. If _metadata* is provided it will be sent before the connection is closed.

```shell
uuid_audio_stream <uuid> pause
```

Pauses audio stream

```shell
uuid_audio_stream <uuid> resume
```

Resumes audio stream

```shell
uuid_audio_stream <uuid> flush
```

Discards audio queued for playback and drops stale in-flight audio received during the following second (barge-in). Use it to interrupt a response that is currently playing out.

## Events

Module will generate the following event types:

- `mod_audio_stream::json`
- `mod_audio_stream::connect`
- `mod_audio_stream::disconnect`
- `mod_audio_stream::error`
- `mod_audio_stream::play`

### response

Message received from websocket endpoint. Json expected, but it contains whatever the websocket server's response is.

#### Freeswitch event generated

**Name**: mod_audio_stream::json
**Body**: WebSocket server response

### connect

Successfully connected to websocket server.

#### Freeswitch event generated

**Name**: mod_audio_stream::connect
**Body**: JSON

```json
{
 "status": "connected"
}
```

### disconnect

Disconnected from websocket server.

#### Freeswitch event generated

**Name**: mod_audio_stream::disconnect
**Body**: JSON

```json
{
 "status": "disconnected",
 "message": {
  "code": 1000,
  "reason": "Normal closure"
 }
}
```

- code: `<int>`
- reason: `<string>`

### error

There is an error with the connection. Multiple fields will be available on the event to describe the error.

#### Freeswitch event generated

**Name**: mod_audio_stream::error
**Body**: JSON

```json
{
 "status": "error",
 "message": {
  "code": 1,
  "error": "String explaining the error"
 }
}
```

- code: `<int>`
- error: `<string>`

#### Possible `code` values

| Code | Enum Name             | Meaning                                              |
|:----:|:----------------------|:-----------------------------------------------------|
| 1    | `IO`                  | I/O error when reading/writing sockets               |
| 2    | `INVALID_HEADER`      | Server sent a malformed WebSocket header             |
| 3    | `SERVER_MASKED`       | Server frames were masked (not allowed by spec)      |
| 4    | `NOT_SUPPORTED`       | Requested feature (e.g. extension) not supported     |
| 5    | `PING_TIMEOUT`        | No PONG received within timeout                      |
| 6    | `CONNECT_FAILED`      | TCP connection or DNS lookup failed                  |
| 7    | `TLS_INIT_FAILED`     | Couldn't initialize SSL/TLS context                  |
| 8    | `SSL_HANDSHAKE_FAILED`| SSL/TLS handshake with server failed                 |
| 9    | `SSL_ERROR`           | Generic OpenSSL error (certificate, cipher, etc.)    |
| 10   | `TIMEOUT`             | Timeout                                              |
| 11   | `PROTOCOL`            | WebSocket protocol error                             |

### Binary PCM playback

The WebSocket server can send raw audio as binary messages. Audio must be signed
16-bit little-endian PCM, without a WAV header, at the sample rate and channel
count selected by `uuid_audio_stream ... start`. For `mono 16k`, send one channel
at 16000 Hz; a 20 ms packet contains 640 bytes. The module resamples it to the
call's codec rate using the same playback path as legacy raw `streamAudio`.
Message boundaries need not align with 20 ms packets, but each message must
contain complete samples for every channel. Empty messages are ignored and
misaligned payloads are dropped with a warning.

Caller audio is already sent upstream as binary PCM, so this allows binary PCM
in both directions without base64 or JSON audio envelopes. Text messages remain
available for control/intent JSON. Existing JSON `streamAudio` remains supported,
including its explicit sample rate for servers generating audio at another rate.
Binary playback generates no `mod_audio_stream::json` or `play` event.

### Server-side playback interruption

For raw PCM playback, the WebSocket server can send a text frame:

```json
{"type":"clearAudio"}
```

This clears decoded audio queued by earlier binary PCM or `streamAudio` messages.
It is handled directly by the module and does not emit a `mod_audio_stream::json` or `play`
event. The next audio message is accepted immediately, including when it follows
the clear on the same connection without a pause. Repeated clears are safe.
The next audio burst resets its resampler history to avoid leaking the previous
response's filter tail.

The server must stop sending the interrupted response before sending `clearAudio`.
Messages on the same WebSocket are ordered: old audio arriving before the clear
is discarded from the playback queue, and audio after it belongs to the next
response. This differs from `uuid_audio_stream <uuid> flush`, which also discards
incoming audio temporarily. Do not use the API's suppression window for an
ordered server-side clear, as it can discard the start of the next response.

Audio already handed to FreeSWITCH/RTP or buffered in the caller's device cannot
be recalled. This operation clears the module's raw PCM queue; it does not stop
file playback triggered by the `play` event.

### play

**Name**: mod_audio_stream::play
**Body**: JSON

Websocket server may return a JSON object containing base64 encoded audio to be played to the caller. To use this feature, response must follow the format:

```json
{
  "type": "streamAudio",
  "data": {
    "audioDataType": "raw",
    "sampleRate": 8000,
    "audioData": "base64 encoded audio"
  }
}
```

- audioDataType: `<raw|wav|mp3|ogg|pcmu|pcma>`

There are two behaviours:

- **`raw`** - `sampleRate` is required and must be one of 8000, 16000, 24000, 32000, 48000 or 64000. The decoded audio is treated as raw signed 16-bit little-endian PCM and injected directly into the call as it arrives; no file is written and no `mod_audio_stream::play` event is generated. Use `flush` to interrupt audio that is currently playing out.
- **file types** (`wav`, `mp3`, `ogg`, `pcmu`, `pcma`) - the decoded audio is written to a temporary file. An event (subclass: _mod_audio_stream::play_) is generated, matching the `data` element with the **file** added to it representing filePath:

```json
{
  "audioDataType": "wav",
  "sampleRate": 8000,
  "file": "/path/to/the/file"
}
```

If printing to the log is not suppressed, `response` printed to the console will look the same as the event. The original response containing base64 encoded audio is replaced because it can be quite huge. Files remain available until the stream is stopped or the call ends, then are deleted.

All the files generated by this feature will reside at the temp directory and will be deleted when the session is closed.

## Example

See `docker/README.md` for complete working setup for testing bi-directional audio.
