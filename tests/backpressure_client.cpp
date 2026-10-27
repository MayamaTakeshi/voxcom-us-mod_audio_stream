#include "WebSocketClient.h"
#include <atomic>
#include <chrono>
#include <cstdint>
#include <iostream>
#include <string>
#include <thread>

static bool waitFor(const std::atomic<bool>& flag) {
    for (int i = 0; i < 5000; ++i) {
        if (flag.load()) return true;
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    return false;
}

int main(int argc, char** argv) {
    if (argc != 3) return 2;
    const std::string mode = argv[2];
    const std::string chunk(65536, 'x');
    std::atomic<bool> opened{false}, release{false}, open_done{false}, overflow{false}, rejected{false};
    std::atomic<int> errors{0}, binary_echoes{0}, text_echoes{0}, accepted_frames{0};
    WebSocketClient client;
    WebSocketTLSOptions tls;
    tls.caFile = "NONE";
    tls.disableHostnameValidation = true;
    client.setTLSOptions(tls);
    client.setUrl(argv[1]);
    client.setConnectionTimeout(3);
    client.setErrorCallback([&](int code, const std::string& message) {
        ++errors;
        if (code == 1 && message.find("Outgoing WebSocket buffer limit exceeded") != std::string::npos) overflow = true;
    });
    client.setBinaryCallback([&](const void*, size_t) { ++binary_echoes; });
    client.setMessageCallback([&](const std::string&) { ++text_echoes; });
    client.setOpenCallback([&] {
        opened = true;
        if (mode == "queue" || mode == "oversized") {
            // Keep the event thread busy while the producer fills its queue.
            waitFor(release);
        } else if (mode == "expansion") {
            // Input fits the raw limit, but incompressible data expands beyond
            // the wire limit when permessage-deflate is negotiated.
            std::string large(1024 * 1024 - 14, '\0');
            uint32_t state = 0x12345678;
            for (auto& byte : large) {
                state ^= state << 13; state ^= state >> 17; state ^= state << 5;
                byte = static_cast<char>(state);
            }
            rejected = !client.sendBinary(large.data(), large.size());
        } else if (mode == "output") {
            // Serialize faster than the event loop can drain its socket. This
            // deterministically tests the transport cap on a real connection.
            for (int i = 0; i < 64; ++i) {
                if (!client.sendBinary(chunk.data(), chunk.size())) { rejected = true; break; }
                ++accepted_frames;
            }
        }
        open_done = true;
    });
    client.connect();
    bool ok = waitFor(opened);
    if (ok && mode == "queue") {
        for (int i = 0; i < 16; ++i) {
            bool accepted = i % 2 ? client.sendMessage(chunk) : client.sendBinary(chunk.data(), chunk.size());
            ok = accepted && ok;
        }
        rejected = !client.sendBinary(chunk.data(), chunk.size());
    } else if (ok && mode == "oversized") {
        const std::string large(1024 * 1024 + 1, 'x');
        rejected = !client.sendMessage(large);
    } else if (ok && mode == "normal") {
        for (int i = 0; i < 10; ++i) {
            ok = client.sendBinary(chunk.data(), 640) && ok;
            ok = client.sendMessage("metadata") && ok;
            std::this_thread::sleep_for(std::chrono::milliseconds(2));
        }
        for (int i = 0; i < 5000 && (binary_echoes != 10 || text_echoes != 10); ++i)
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
        ok = ok && binary_echoes == 10 && text_echoes == 10 && errors == 0;
    }
    release = true;
    ok = waitFor(open_done) && ok;
    if (mode == "output") ok = ok && accepted_frames == 15;
    if (mode != "normal") {
        ok = waitFor(overflow) && ok;
        ok = ok && rejected && !client.sendMessage("after overflow");
    }
    client.disconnect();
    ok = ok && (mode == "normal" ? errors == 0 : errors == 1);
    std::cout << mode << ": " << (ok ? "PASS" : "FAIL") << " errors=" << errors
              << " rejected=" << rejected << std::endl;
    return ok ? 0 : 1;
}
