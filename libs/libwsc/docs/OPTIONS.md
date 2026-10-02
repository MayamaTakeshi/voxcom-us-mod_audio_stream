## Additional Options

After setting up your URL and callbacks, you can tweak these behaviors before calling `connect()`:

- **Compression**  
  per-message deflate is enabled by default and offered in websocket handshake. To disable it use:

  ```cpp
  client.enableCompression(false);
  ```

- **Ping interval**  
  Disabled by default.

  ```cpp
  client.setPingInterval(3);
  ```

- **Connection timeout**  
  Default is 2s. You can tweak it by using:

  ```cpp
  client.setConnectionTimeout(3);
  ```

- **Custom HTTP Headers**  
  Add or override any handshake headers:

  ```cpp
  WebSocketHeaders hdrs;
  hdrs.set("X-My-Header", "Value");
  hdrs.set("User-Agent", "libwsc/1.0.0");
  client.setHeaders(hdrs);
  ```

- **TLS Settings**  
  Provide certificates, cipher suites, and peer-verification options via `WebSocketTLSOptions`.
  Other options remain at their defaults.

  ```cpp
  WebSocketTLSOptions tls;
  tls.certFile                = "/path/to/client.crt";
  tls.keyFile                 = "/path/to/client.key";
  tls.caFile                  = "NONE";               // disable peer verification
  tls.ciphers                 = WebSocketTLSOptions::getDefaultCiphers();
  tls.disableHostnameValidation = true;
  client.setTlsOptions(tls);
  ```

  - Setting `tls.caFile = "NONE"` alone is enough to disable peer verification, and that all other fields will fall back to their defaults.