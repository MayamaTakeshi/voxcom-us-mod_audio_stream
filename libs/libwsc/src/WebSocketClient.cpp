/*
 *  WebSocketClient.cpp
 *  Author: Milan M.
 *  Copyright (c) 2025 AMSOFTSWITCH LTD. All rights reserved.
 */

#include "WebSocketClient.h"
#include "WebSocketContext.h"

#include <arpa/inet.h>
#include <iomanip>

WebSocketClient::WebSocketClient() = default;

WebSocketClient::~WebSocketClient() {
    disconnect();
}

bool WebSocketClient::isConnected() {
    std::shared_ptr<WebSocketContext> ctx;
    {
        std::lock_guard<std::mutex> lk(*_ctx_mutex);
        ctx = _ctx;
    }
    return ctx && ctx->isConnected();
}

void WebSocketClient::setUrl(const std::string& url) {
    const std::string ws_scheme = "ws://";
    const std::string wss_scheme = "wss://";

    size_t pos = 0;
    if (url.compare(0, ws_scheme.size(), ws_scheme) == 0) {
        secure = false;
        pos = ws_scheme.size();
    } else if (url.compare(0, wss_scheme.size(), wss_scheme) == 0) {
        secure = true;
        pos = wss_scheme.size();
    } else {
        return;
    }

    size_t path_pos = url.find_first_of("/?#", pos);
    std::string hostport = (path_pos == std::string::npos) ? url.substr(pos) : url.substr(pos, path_pos - pos);
    std::string port_text;

    if (!hostport.empty() && hostport[0] == '[') {
        const size_t close_bracket = hostport.find(']');
        if (close_bracket == std::string::npos) return;
        host = hostport.substr(1, close_bracket - 1);
        if (close_bracket + 1 < hostport.size()) {
            if (hostport[close_bracket + 1] != ':') return;
            port_text = hostport.substr(close_bracket + 2);
        }
    } else {
        const size_t colon_pos = hostport.find(':');
        if (colon_pos != std::string::npos) {
            if (hostport.find(':', colon_pos + 1) != std::string::npos) return;
            host = hostport.substr(0, colon_pos);
            port_text = hostport.substr(colon_pos + 1);
        } else {
            host = hostport;
        }
    }

    if (!port_text.empty()) {
        try {
            size_t parsed = 0;
            const int parsed_port = std::stoi(port_text, &parsed);
            if (parsed != port_text.size() || parsed_port < 1 || parsed_port > 65535) return;
            port = static_cast<unsigned short>(parsed_port);
        } catch (const std::exception&) {
            return;
        }
    } else {
        if (hostport.find(':') != std::string::npos && hostport.back() == ':') return;
        port = secure ? 443 : 80;
    }

    if (host.empty()) {
        return;
    }

    if (path_pos == std::string::npos) {
        uri = "/";
    } else if (url[path_pos] == '/' ) {
        uri = url.substr(path_pos);
    } else {
        uri = "/" + url.substr(path_pos);
    }

    is_ip_address = isHostIPAddress(host);
}

bool WebSocketClient::isHostIPAddress(const std::string& host) {
    struct in_addr addr4;
    if (inet_pton(AF_INET, host.c_str(), &addr4) == 1) {
        return true;
    }
    
    struct in6_addr addr6;
    std::string host_clean = host;
    
    if (host.size() >= 2 && host[0] == '[' && host[host.size()-1] == ']') {
        host_clean = host.substr(1, host.size() - 2);
    }
    
    if (inet_pton(AF_INET6, host_clean.c_str(), &addr6) == 1) {
        return true;
    }
    
    // If it's not a valid IP address, it's a domain name
    return false;
}

void WebSocketClient::setHeaders(const WebSocketHeaders& headers) {
    extra_headers = headers;
}

void WebSocketClient::setTLSOptions(const WebSocketTLSOptions& options) {
    tls_options = options;
}

void WebSocketClient::setPingInterval(int interval) {
    ping_interval = interval;
}

void WebSocketClient::setConnectionTimeout(int timeout) {
    connection_timeout = timeout;
}

void WebSocketClient::enableCompression(bool enable) {
    compression_requested = enable;
}

void WebSocketClient::setOpenCallback(OpenCallback callback) {
    std::shared_ptr<WebSocketContext> ctx;
    OpenCallback cb;
    {
        std::lock_guard<std::mutex> lk(*_ctx_mutex);
        open_callback = std::move(callback);
        cb = open_callback;
        ctx = _ctx;
    }
    if (ctx) ctx->setOpenCallback(std::move(cb));
}

void WebSocketClient::setCloseCallback(CloseCallback callback) {
    std::shared_ptr<WebSocketContext> ctx;
    CloseCallback cb;
    {
        std::lock_guard<std::mutex> lk(*_ctx_mutex);
        close_callback = std::move(callback);
        cb = close_callback;
        ctx = _ctx;
    }
    if (ctx) ctx->setCloseCallback(std::move(cb));
}

void WebSocketClient::setErrorCallback(ErrorCallback callback) {
    std::shared_ptr<WebSocketContext> ctx;
    ErrorCallback cb;
    {
        std::lock_guard<std::mutex> lk(*_ctx_mutex);
        error_callback = std::move(callback);
        cb = error_callback;
        ctx = _ctx;
    }
    if (ctx) ctx->setErrorCallback(std::move(cb));
}

void WebSocketClient::setMessageCallback(MessageCallback callback) {
    std::shared_ptr<WebSocketContext> ctx;
    MessageCallback cb;
    {
        std::lock_guard<std::mutex> lk(*_ctx_mutex);
        message_callback = std::move(callback);
        cb = message_callback;
        ctx = _ctx;
    }
    if (ctx) ctx->setMessageCallback(std::move(cb));
}

void WebSocketClient::setBinaryCallback(BinaryCallback callback) {
    std::shared_ptr<WebSocketContext> ctx;
    BinaryCallback cb;
    {
        std::lock_guard<std::mutex> lk(*_ctx_mutex);
        binary_callback = std::move(callback);
        cb = binary_callback;
        ctx = _ctx;
    }
    if (ctx) ctx->setBinaryCallback(std::move(cb));
}

bool WebSocketClient::sendMessage(const std::string& message) {
    std::shared_ptr<WebSocketContext> ctx;
    {
        std::lock_guard<std::mutex> lk(*_ctx_mutex);
        ctx = _ctx;
    }
    return ctx && ctx->sendData(message.data(), message.size(), MessageType::TEXT);
}

bool WebSocketClient::sendMessage(const char* msg, size_t len) {
    std::shared_ptr<WebSocketContext> ctx;
    {
        std::lock_guard<std::mutex> lk(*_ctx_mutex);
        ctx = _ctx;
    }
    return ctx && ctx->sendData(msg, len, MessageType::TEXT);
}

bool WebSocketClient::sendBinary(const void* data, size_t length) {
    std::shared_ptr<WebSocketContext> ctx;
    {
        std::lock_guard<std::mutex> lk(*_ctx_mutex);
        ctx = _ctx;
    }
    return ctx && ctx->sendData(data, length, MessageType::BINARY);
}

void WebSocketClient::connect() {
    std::lock_guard<std::mutex> lk(*_ctx_mutex);
    if (_ctx) {
        return;
    }

    WebSocketContext::Config cfg;
    cfg.host = host;
    cfg.port = port;
    cfg.uri = uri;
    cfg.secure = secure;
    cfg.is_ip_address = is_ip_address;
    cfg.ping_interval = ping_interval;
    cfg.connection_timeout = connection_timeout;
    cfg.headers = extra_headers;
    cfg.tls = tls_options;
    cfg.compression_requested = compression_requested;

    try {
        auto ctx = std::make_shared<WebSocketContext>(cfg);
        if (open_callback) ctx->setOpenCallback(open_callback);
        if (close_callback) ctx->setCloseCallback(close_callback);
        if (error_callback) ctx->setErrorCallback(error_callback);
        if (message_callback) ctx->setMessageCallback(message_callback);
        if (binary_callback) ctx->setBinaryCallback(binary_callback);

        _ctx = ctx;
        _ctx->start();

    } catch (...) {
        // Failed to create or start context.
        // Client remains disconnected; user may retry connect().
    }
}

void WebSocketClient::disconnect() {
    if (!_ctx_mutex) return;
    std::shared_ptr<WebSocketContext> ctx;
    {
        std::lock_guard<std::mutex> lk(*_ctx_mutex);
        ctx = std::move(_ctx);
    }
    if (ctx) ctx->stop();
}
