#include <string>
#include <cstring>
#include <cstdio>
#include <cstdlib>
#include "mod_audio_stream.h"
#include "WebSocketClient.h"
#include <switch_json.h>
#include <fstream>
#include <switch_buffer.h>
#include <unordered_set>
#include <atomic>
#include <deque>
#include <vector>
#include <memory>
#include <mutex>
#include <thread>
#include "base64.h"

#define FRAME_SIZE_8000  320 /* 1000x0.02 (20ms)= 160 x(16bit= 2 bytes) 320 frame size*/

/* Escape a string for embedding inside a double-quoted JSON string. Used for
   the Gemini kickstart prompt (channel var STREAM_GEMINI_KICKSTART) which the
   caller controls and may contain quotes/newlines. */
static std::string json_escape(const std::string &s)
{
    std::string out;
    out.reserve(s.size() + 16);
    for (unsigned char c : s)
    {
        switch (c)
        {
        case '"':
            out += "\\\"";
            break;
        case '\\':
            out += "\\\\";
            break;
        case '\n':
            out += "\\n";
            break;
        case '\r':
            out += "\\r";
            break;
        case '\t':
            out += "\\t";
            break;
        case '\b':
            out += "\\b";
            break;
        case '\f':
            out += "\\f";
            break;
        default:
            if (c < 0x20)
            {
                char buf[8];
                snprintf(buf, sizeof(buf), "\\u%04x", c);
                out += buf;
            }
            else
            {
                out += (char)c;
            }
        }
    }
    return out;
}

class AudioStreamer {
public:
    // Factory
    static std::shared_ptr<AudioStreamer> create(
        const char* uuid, const char* wsUri, responseHandler_t callback, int deflate, int heart_beat,
        bool suppressLog, const char* extra_headers, bool no_reconnect,
        const char* tls_cafile, const char* tls_keyfile,
        const char* tls_certfile, bool tls_disable_hostname_validation,
        bool geminiMode, int geminiOutputRate, const char* geminiKickstart) {

        std::shared_ptr<AudioStreamer> sp(new AudioStreamer(
            uuid, wsUri, callback, deflate, heart_beat,
            suppressLog, extra_headers, no_reconnect,
            tls_cafile, tls_keyfile,
            tls_certfile, tls_disable_hostname_validation,
            geminiMode, geminiOutputRate, geminiKickstart
        ));

        sp->bindCallbacks(std::weak_ptr<AudioStreamer>(sp));

        sp->client.connect();

        return sp;
    }

    ~AudioStreamer()= default;

    void disconnect() {
        switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_DEBUG, "disconnecting...\n");
        client.disconnect();
    }

    /** Barge-in flush: drop all incoming audio until the server sends its
     *  first non-audio (JSON) message, which signals the old TTS response
     *  has ended.  Any audio frames that arrive while the flag is set are
     *  silently discarded in eventCallback before reaching write_sbuffer. */
    void discardIncomingAudio() {
        m_discardAudio.store(true, std::memory_order_release);
    }

    bool isConnected() {
        return client.isConnected();
    }

    void writeBinary(uint8_t* buffer, size_t len) {
        if(!this->isConnected()) return;
        client.sendBinary(buffer, len);
    }

    void writeText(const char* text) {
        if(!this->isConnected()) return;
        client.sendMessage(text, strlen(text));
    }

    /* True when this connection speaks the Gemini Live (BidiGenerateContent)
       protocol instead of the plain binary-PCM/streamAudio wire protocol. */
    bool isGeminiMode() {
        return m_geminiMode;
    }

    /* Gemini framing: wrap cooked caller PCM (already resampled to the
       configured ws sampling rate) into a `realtimeInput` JSON frame. The
       protocol forbids sending anything but the setup message before
       `setupComplete` arrives, so frames received in that window are queued and
       flushed by onSetupComplete(). */
    void sendUpstreamAudio(const uint8_t* data, size_t len, int rate) {
        if (!m_geminiMode || len == 0) return;
        std::string b64 = base64_encode(data, len, false);
        char mime[64];
        snprintf(mime, sizeof(mime), "audio/pcm;rate=%d", rate);
        std::string json = "{\"realtimeInput\":{\"audio\":{\"mimeType\":\"";
        json += mime;
        json += "\",\"data\":\"";
        json += b64;
        json += "\"}}}";
        enqueueUpstream(json);
    }

    /* Gemini: the server accepted the setup. Send the optional kickstart
       greeting first (so the model starts talking), then any caller audio that
       accumulated while setup was pending. */
    void onSetupComplete() {
        m_setupDone.store(true, std::memory_order_release);

        std::deque<std::string> pending;
        {
            std::lock_guard<std::mutex> lk(m_stateMutex);
            pending.swap(m_pendingUpstream);
        }

        if (!m_geminiKickstart.empty()) {
            std::string kick = std::string("{\"clientContent\":{\"turns\":[{\"role\":\"user\",\"parts\":[{\"text\":\"")
                + json_escape(m_geminiKickstart) + "\"}]}],\"turnComplete\":true}}";
            writeText(kick.c_str());
        }
        for (auto &m : pending) {
            writeText(m.c_str());
        }
    }

    /* Gemini: polite end-of-stream cue before the websocket closes, so the
       model finalizes the last turn/inference. */
    void sendAudioStreamEnd() {
        if (!m_geminiMode) return;
        if (m_setupDone.load(std::memory_order_acquire)) {
            writeText("{\"realtimeInput\":{\"audioStreamEnd\":true}}");
        }
    }

    void deleteFiles() {
        std::vector<std::string> files;

        {
            std::lock_guard<std::mutex> lk(m_stateMutex);
            if (m_Files.empty())
                return;

            files.assign(m_Files.begin(), m_Files.end());
            m_Files.clear();
            m_playFile = 0;
        }

        for (const auto& fn : files) {
            ::remove(fn.c_str());
        }
    }

    void markCleanedUp() {
        m_cleanedUp.store(true, std::memory_order_release);
        client.setMessageCallback({});
        client.setOpenCallback({});
        client.setErrorCallback({});
        client.setCloseCallback({});
    }

    bool isCleanedUp() const {
        return m_cleanedUp.load(std::memory_order_acquire);
    }

private:
    // Ctor
    AudioStreamer(
        const char* uuid, const char* wsUri, responseHandler_t callback, int deflate, int heart_beat,
        bool suppressLog, const char* extra_headers, bool no_reconnect,
        const char* tls_cafile, const char* tls_keyfile,
        const char* tls_certfile, bool tls_disable_hostname_validation,
        bool geminiMode, int geminiOutputRate, const char* geminiKickstart
    ) : m_sessionId(uuid), m_notify(callback), m_suppress_log(suppressLog),
        m_extra_headers(extra_headers), m_playFile(0),
        m_geminiMode(geminiMode), m_geminiOutputRate(geminiOutputRate),
        m_geminiKickstart(geminiKickstart ? geminiKickstart : "") {
        (void)no_reconnect; // libwsc has no auto-reconnect; flag retained for API compatibility

        WebSocketHeaders hdrs;
        WebSocketTLSOptions tls;

        if (m_extra_headers) {
            cJSON *headers_json = cJSON_Parse(m_extra_headers);
            if (headers_json) {
                cJSON *iterator = headers_json->child;
                while (iterator) {
                    if (iterator->type == cJSON_String && iterator->valuestring != nullptr) {
                        hdrs.set(iterator->string, iterator->valuestring);
                    }
                    iterator = iterator->next;
                }
                cJSON_Delete(headers_json);
            }
        }

        client.setUrl(wsUri);

        // Setup TLS options
        // NONE - disables validation
        // SYSTEM - uses the system CAs bundle
        if (tls_cafile) {
            tls.caFile = tls_cafile;
        }

        if (tls_keyfile) {
            tls.keyFile = tls_keyfile;
        }

        if (tls_certfile) {
            tls.certFile = tls_certfile;
        }

        tls.disableHostnameValidation = tls_disable_hostname_validation;
        client.setTLSOptions(tls);

        // Optional heart beat, sent every xx seconds when there is not any traffic
        // to make sure that load balancers do not kill an idle connection.
        if(heart_beat)
            client.setPingInterval(heart_beat);

        // Per message deflate connection is enabled by default. You can tweak its parameters or disable it
        if(deflate)
            client.enableCompression(false);

        // Set extra headers if any
        if(!hdrs.empty())
            client.setHeaders(hdrs);
    }

    struct ProcessResult {
        switch_bool_t ok = SWITCH_FALSE;
        std::string rewrittenJsonData;
        std::vector<std::string> errors;
        bool isRawAudio = false;
        int sampleRate = 0;
        std::vector<uint8_t> rawAudio;
    };

    static inline void push_err(ProcessResult& out, const std::string& sid, const std::string& s) {
        out.errors.push_back("(" + sid + ") " + s);
    }

    void bindCallbacks(std::weak_ptr<AudioStreamer> wp) {
        client.setMessageCallback([wp](const std::string& message) {
            auto self = wp.lock();
            if (!self) return;
            if (self->isCleanedUp()) return;
            self->eventCallback(MESSAGE, message.c_str());
        });

        client.setOpenCallback([wp]() {
            auto self = wp.lock();
            if (!self) return;
            if (self->isCleanedUp()) return;

            cJSON* root = cJSON_CreateObject();
            cJSON_AddStringToObject(root, "status", "connected");
            char* json_str = cJSON_PrintUnformatted(root);

            self->eventCallback(CONNECT_SUCCESS, json_str);

            cJSON_Delete(root);
            switch_safe_free(json_str);
        });

        client.setErrorCallback([wp](int code, const std::string& msg) {
            auto self = wp.lock();
            if (!self) return;
            if (self->isCleanedUp()) return;

            cJSON* root = cJSON_CreateObject();
            cJSON_AddStringToObject(root, "status", "error");
            cJSON* message = cJSON_CreateObject();
            cJSON_AddNumberToObject(message, "code", code);
            cJSON_AddStringToObject(message, "error", msg.c_str());
            cJSON_AddItemToObject(root, "message", message);

            char* json_str = cJSON_PrintUnformatted(root);

            self->eventCallback(CONNECT_ERROR, json_str);

            cJSON_Delete(root);
            switch_safe_free(json_str);
        });

        client.setCloseCallback([wp](int code, const std::string& reason) {
            auto self = wp.lock();
            if (!self) return;
            if (self->isCleanedUp()) return;

            cJSON* root = cJSON_CreateObject();
            cJSON_AddStringToObject(root, "status", "disconnected");
            cJSON* message = cJSON_CreateObject();
            cJSON_AddNumberToObject(message, "code", code);
            cJSON_AddStringToObject(message, "reason", reason.c_str());
            cJSON_AddItemToObject(root, "message", message);

            char* json_str = cJSON_PrintUnformatted(root);

            self->eventCallback(CONNECTION_DROPPED, json_str);

            cJSON_Delete(root);
            switch_safe_free(json_str);
        });
    }

    switch_media_bug_t *get_media_bug(switch_core_session_t *session) {
        switch_channel_t *channel = switch_core_session_get_channel(session);
        if(!channel) {
            return nullptr;
        }
        auto *bug = (switch_media_bug_t *) switch_channel_get_private(channel, MY_BUG_NAME);
        return bug;
    }

    inline void media_bug_close(switch_core_session_t *session) {
        auto *bug = get_media_bug(session);
        if(bug) {
            auto* tech_pvt = (private_t*) switch_core_media_bug_get_user_data(bug);
            tech_pvt->close_requested = 1;
            switch_core_media_bug_close(&bug, SWITCH_FALSE);
        }
    }

    inline void send_initial_metadata(switch_core_session_t *session) {
        auto *bug = get_media_bug(session);
        if(bug) {
            auto* tech_pvt = (private_t*) switch_core_media_bug_get_user_data(bug);
            if(tech_pvt && strlen(tech_pvt->initialMetadata) > 0) {
                switch_log_printf(SWITCH_CHANNEL_SESSION_LOG(session), SWITCH_LOG_DEBUG,
                                          "sending initial metadata %s\n", tech_pvt->initialMetadata);
                writeText(tech_pvt->initialMetadata);
            }
        }
    }

    /* Gemini pre-setup window: the protocol rejects any client frame before
       setupComplete. Queue the JSON text frames in order and flush them once
       onSetupComplete() fires. The queue is bounded so a dead/never-replying
       peer cannot grow memory without limit. */
    void enqueueUpstream(const std::string& json) {
        if (m_setupDone.load(std::memory_order_acquire)) {
            writeText(json.c_str());
            return;
        }
        std::lock_guard<std::mutex> lk(m_stateMutex);
        m_pendingUpstream.push_back(json);
        /* ~20 ms frames; cap at ~10 s of buffered audio. */
        while (m_pendingUpstream.size() > 500) {
            m_pendingUpstream.pop_front();
        }
    }

    void injectRawAudio(switch_core_session_t *session, const std::vector<uint8_t>& rawAudio, int sampleRate) {
        auto *bug = get_media_bug(session);
        if (!bug) return;

        auto *tech_pvt = (private_t *)switch_core_media_bug_get_user_data(bug);
        if (!tech_pvt || tech_pvt->close_requested || !tech_pvt->write_sbuffer) return;

        const int outRate = tech_pvt->sampling;
        const int channels = tech_pvt->channels;
        const int inRate = sampleRate ? sampleRate : tech_pvt->wsSampling;

        if (rawAudio.empty() || channels <= 0) return;

        spx_uint32_t in_frames = (spx_uint32_t)(rawAudio.size() / (sizeof(spx_int16_t) * channels));
        if (in_frames == 0) return;

        spx_uint32_t max_out = (spx_uint32_t)((double)in_frames * outRate / inRate) + 1;
        std::vector<spx_int16_t> in_buf(in_frames * channels);
        std::vector<spx_int16_t> out_buf(max_out * channels);
        std::memcpy(in_buf.data(), rawAudio.data(), rawAudio.size());

        spx_uint32_t in_len = in_frames;
        spx_uint32_t out_len = max_out;

        if (inRate == outRate || !tech_pvt->write_resampler) {
            // no resample needed - copy through
            out_buf.assign(in_buf.begin(), in_buf.end());
            out_len = in_len;
        } else if (channels == 1) {
            speex_resampler_process_int(tech_pvt->write_resampler, 0,
                                        in_buf.data(), &in_len,
                                        out_buf.data(), &out_len);
        } else {
            speex_resampler_process_interleaved_int(tech_pvt->write_resampler,
                                                    in_buf.data(), &in_len,
                                                    out_buf.data(), &out_len);
        }

        const size_t bytes_out = (size_t)out_len * (size_t)channels * sizeof(spx_int16_t);

        if (switch_mutex_lock(tech_pvt->write_mutex) != SWITCH_STATUS_SUCCESS) {
            switch_log_printf(SWITCH_CHANNEL_SESSION_LOG(session), SWITCH_LOG_WARNING,
                              "%s injectRawAudio: write mutex lock failed, dropping %zu bytes\n",
                              tech_pvt->sessionId, bytes_out);
            return;
        }

        size_t remaining = bytes_out;
        const uint8_t *ptr = reinterpret_cast<const uint8_t *>(out_buf.data());
        while (remaining > 0) {
            /* Never spin here once teardown starts: we hold the session read lock
               (from eventCallback), and nothing is draining write_sbuffer any more. */
            if (tech_pvt->close_requested || tech_pvt->cleanup_started) {
                break;
            }
            switch_size_t free_space = switch_buffer_freespace(tech_pvt->write_sbuffer);
            if (free_space == 0) {
                switch_mutex_unlock(tech_pvt->write_mutex);
                switch_yield(10000);
                if (switch_mutex_lock(tech_pvt->write_mutex) != SWITCH_STATUS_SUCCESS) return;
                continue;
            }
            size_t chunk = std::min<size_t>(remaining, free_space);
            switch_buffer_write(tech_pvt->write_sbuffer, ptr, chunk);
            ptr += chunk;
            remaining -= chunk;
        }

        switch_mutex_unlock(tech_pvt->write_mutex);
    }

    void eventCallback(notifyEvent_t event, const char* message) {
        std::string msg = message ? message : "";

        // processing without holding a session
        ProcessResult pr;
        if (event == MESSAGE) {
            pr = processMessage(msg);
            if (pr.ok == SWITCH_TRUE) {
                msg = pr.rewrittenJsonData; // overwrite only on success
            }
        }

        switch_core_session_t* psession = switch_core_session_locate(m_sessionId.c_str());
        if (!psession) {
            return;
        }

        for (const auto& e : pr.errors) {
            switch_log_printf(SWITCH_CHANNEL_SESSION_LOG(psession), SWITCH_LOG_ERROR, "%s\n", e.c_str());
        }

        switch (event) {
            case CONNECT_SUCCESS:
                send_initial_metadata(psession);
                m_notify(psession, EVENT_CONNECT, msg.c_str());
                break;

            case CONNECTION_DROPPED:
                switch_log_printf(SWITCH_CHANNEL_SESSION_LOG(psession), SWITCH_LOG_INFO, "connection closed\n");
                m_notify(psession, EVENT_DISCONNECT, msg.c_str());
                break;

            case CONNECT_ERROR:
                switch_log_printf(SWITCH_CHANNEL_SESSION_LOG(psession), SWITCH_LOG_INFO, "connection error\n");
                m_notify(psession, EVENT_ERROR, msg.c_str());
                media_bug_close(psession);
                break;

            case MESSAGE:
                if (pr.isRawAudio) {
                    if (m_discardAudio.load(std::memory_order_acquire)) {
                        // Barge-in flush active: silently drop audio that
                        // belonged to the interrupted response.
                        break;
                    }
                    /* Don't inject into a dying channel - nothing drains it. */
                    {
                        switch_channel_t *ch = switch_core_session_get_channel(psession);
                        if (ch && switch_channel_ready(ch)) {
                            injectRawAudio(psession, pr.rawAudio, pr.sampleRate);
                        }
                    }
                } else {
                    // Any non-audio message from the server means the old TTS
                    // response has ended; re-enable audio injection.
                    m_discardAudio.store(false, std::memory_order_release);
                    if (pr.ok == SWITCH_TRUE) {
                        m_notify(psession, EVENT_PLAY, msg.c_str());
                    } else {
                        // fall back to EVENT_JSON
                        m_notify(psession, EVENT_JSON, msg.c_str());
                    }
                }

                if (!m_suppress_log && !pr.isRawAudio) {
                    switch_log_printf(SWITCH_CHANNEL_SESSION_LOG(psession), SWITCH_LOG_DEBUG,
                                    "response: %s\n", msg.c_str());
                }
                break;
        }

        switch_core_session_rwunlock(psession);
    }


    ProcessResult processMessage(const std::string& message) {
        ProcessResult out;

        if (isCleanedUp()) return out;

        // RAII
        using jsonPtr = std::unique_ptr<cJSON, decltype(&cJSON_Delete)>;
        jsonPtr root(cJSON_Parse(message.c_str()), &cJSON_Delete);
        if (!root) return out;

        // Gemini Live framing: the server speaks the native
        // BidiGenerateContent protocol (no "type": "streamAudio" envelope).
        if (m_geminiMode) {
            return processGeminiMessage(root.get(), message);
        }

        const char* jsonType = cJSON_GetObjectCstr(root.get(), "type");
        if (!jsonType || std::strcmp(jsonType, "streamAudio") != 0) {
            return out; // not ours
        }

        cJSON* jsonData = cJSON_GetObjectItem(root.get(), "data");
        if (!jsonData) {
            push_err(out, m_sessionId, "processMessage - no data in streamAudio");
            return out;
        }

        const char* jsAudioDataType = cJSON_GetObjectCstr(jsonData, "audioDataType");
        if (!jsAudioDataType) jsAudioDataType = "";

        jsonPtr jsonAudio(cJSON_DetachItemFromObject(jsonData, "audioData"), &cJSON_Delete);

        if (!jsonAudio) {
            push_err(out, m_sessionId, "processMessage - streamAudio missing 'audioData' field");
            return out;
        }

        if (!cJSON_IsString(jsonAudio.get()) || !jsonAudio->valuestring) {
            push_err(out, m_sessionId, "processMessage - 'audioData' is not a string (expected base64 string)");
            return out;
        }

        // sampleRate (only meaningful for raw)
        int sampleRate = 0;
        if (cJSON* jsonSampleRate = cJSON_GetObjectItem(jsonData, "sampleRate")) {
            sampleRate = jsonSampleRate->valueint;
        }

        const bool isRaw = std::strcmp(jsAudioDataType, "raw") == 0;

        // map file type (raw is handled out-of-band via write buffer; see eventCallback)
        std::string fileType;
        if (isRaw) {
            switch (sampleRate) {
                case 8000:
                case 16000:
                case 24000:
                case 32000:
                case 48000:
                case 64000:
                    break;
                default:
                    push_err(out, m_sessionId, "processMessage - unsupported sample rate: " + std::to_string(sampleRate));
                    return out;
            }
        } else if (std::strcmp(jsAudioDataType, "wav") == 0)  fileType = ".wav";
        else if (std::strcmp(jsAudioDataType, "mp3") == 0)   fileType = ".mp3";
        else if (std::strcmp(jsAudioDataType, "ogg") == 0)   fileType = ".ogg";
        else if (std::strcmp(jsAudioDataType, "pcmu") == 0)  fileType = ".pcmu";
        else if (std::strcmp(jsAudioDataType, "pcma") == 0)  fileType = ".pcma";
        else {
            push_err(out, m_sessionId, "processMessage - unsupported audio type: " + std::string(jsAudioDataType));
            return out;
        }

        // base64 decode
        std::string decoded;
        try {
            decoded = base64_decode(jsonAudio->valuestring);
        } catch (const std::exception& e) {
            push_err(out, m_sessionId, "processMessage - base64 decode error: " + std::string(e.what()));
            return out;
        }

        if (isRaw) {
            out.isRawAudio = true;
            out.sampleRate = sampleRate;
            out.rawAudio.assign(
                reinterpret_cast<const uint8_t*>(decoded.data()),
                reinterpret_cast<const uint8_t*>(decoded.data()) + decoded.size());
            return out;
        }

        // reserve file index
        int idx = 0;
        {
            std::lock_guard<std::mutex> lk(m_stateMutex);
            idx = m_playFile++;
        }

        char filePath[256];
        switch_snprintf(filePath, sizeof(filePath), "%s%s%s_%d.tmp%s",
                        SWITCH_GLOBAL_dirs.temp_dir, SWITCH_PATH_SEPARATOR,
                        m_sessionId.c_str(), idx, fileType.c_str());

        // write file
        {
            std::ofstream f(filePath, std::ios::binary);
            if (!f.is_open()) {
                push_err(out, m_sessionId, std::string("processMessage - failed to open file for write: ") + filePath);
                return out;
            }
            f.write(decoded.data(), static_cast<std::streamsize>(decoded.size()));
            if (!f.good()) {
                push_err(out, m_sessionId, std::string("processMessage - failed writing file: ") + filePath);
                return out;
            }
        }

        // track file for cleanup
        {
            std::lock_guard<std::mutex> lk(m_stateMutex);
            m_Files.insert(filePath);
        }

        cJSON_AddItemToObject(jsonData, "file", cJSON_CreateString(filePath));

        // return rewritten jsonData as string
        char* jsonString = cJSON_PrintUnformatted(jsonData);
        if (!jsonString) {
            push_err(out, m_sessionId, "processMessage - cJSON_PrintUnformatted failed");
            return out;
        }

        out.rewrittenJsonData.assign(jsonString);
        std::free(jsonString);
        out.ok = SWITCH_TRUE;
        return out;
    }

    /* Parse a raw Gemini Live server frame. Recognized shapes:
       reason: cancel / error / finished / maxTurns (end the session), otherwise
       reason: unspecified/startOfTurn/etc. (keep running). We do not end the
       session here: the caller decides when a call finishes. */
    ProcessResult processGeminiMessage(cJSON* root, const std::string& message) {
        ProcessResult out;

        // setupComplete: the server accepted our setup; flush pending upstream.
        if (cJSON_GetObjectItem(root, "setupComplete")) {
            onSetupComplete();
            switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_INFO,
                              "gemini session %s: setup complete\n", m_sessionId.c_str());
        }

        // server errors: surface them via the err handling in eventCallback.
        cJSON* jsonError = cJSON_GetObjectItem(root, "error");
        if (jsonError) {
            const char* em = cJSON_GetObjectCstr(jsonError, "message");
            push_err(out, m_sessionId,
                     std::string("Gemini error: ") + (em ? em : "<no message>"));
            return out;
        }

        // serverContent: model audio / turn lifecycle.
        cJSON* serverContent = cJSON_GetObjectItem(root, "serverContent");
        if (serverContent) {
            cJSON* interrupted = cJSON_GetObjectItem(serverContent, "interrupted");
            if (interrupted && cJSON_IsTrue(interrupted)) {
                switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_INFO,
                                  "gemini session %s: turn interrupted (barge-in)\n",
                                  m_sessionId.c_str());
                /* The in-flight injected audio belongs to the cut-off turn; we
                   leave the write buffer alone and rely on the next turn's
                   audio. */
            }

            cJSON* modelTurn = cJSON_GetObjectItem(serverContent, "modelTurn");
            if (modelTurn) {
                cJSON* parts = cJSON_GetObjectItem(modelTurn, "parts");
                if (parts && cJSON_IsArray(parts)) {
                    std::string rawAudio;
                    int sampleRate = m_geminiOutputRate;
                    bool decodeError = false;
                    int count = cJSON_GetArraySize(parts);
                    for (int i = 0; i < count; i++) {
                        cJSON* part = cJSON_GetArrayItem(parts, i);
                        if (!part) continue;
                        cJSON* inlineData = cJSON_GetObjectItem(part, "inlineData");
                        if (!inlineData) continue; /* e.g. only text parts */
                        const char* data = cJSON_GetObjectCstr(inlineData, "data");
                        if (!data) continue;
                        const char* mime = cJSON_GetObjectCstr(inlineData, "mimeType");
                        if (mime) {
                            /* mimeType: "audio/pcm;rate=24000" */
                            const char* p = std::strstr(mime, "rate=");
                            if (p) {
                                int r = std::atoi(p + 5);
                                if (r > 0) sampleRate = r;
                            }
                        }
                        try {
                            std::string decoded = base64_decode(data);
                            rawAudio += decoded;
                        } catch (const std::exception& e) {
                            decodeError = true;
                            push_err(out, m_sessionId,
                                     std::string("Gemini base64 decode error: ") + e.what());
                            break;
                        }
                    }
                    if (!decodeError && !rawAudio.empty()) {
                        out.isRawAudio = true;
                        out.sampleRate = sampleRate;
                        out.rawAudio.assign(
                            reinterpret_cast<const uint8_t*>(rawAudio.data()),
                            reinterpret_cast<const uint8_t*>(rawAudio.data()) + rawAudio.size());
                    }
                }
            }
            return out;
        }

        // Everything else (e.g. usageMetadata-only frames, keepalive) is a no-op.
        return out;
    }

private:
    std::string m_sessionId;
    responseHandler_t m_notify;
    WebSocketClient client;
    bool m_suppress_log;
    const char* m_extra_headers;
    int m_playFile;
    std::unordered_set<std::string> m_Files;
    std::atomic<bool> m_cleanedUp{false};
    std::mutex m_stateMutex;
    // Barge-in: drop incoming raw audio until the server sends a non-audio
    // message (which signals the old TTS response has ended).
    std::atomic<bool> m_discardAudio{false};
    // Gemini Live (BidiGenerateContent) framing state.
    bool m_geminiMode = false;
    int m_geminiOutputRate = 24000;
    std::string m_geminiKickstart;
    std::atomic<bool> m_setupDone{false};
    std::deque<std::string> m_pendingUpstream;
};


namespace {

    /* Session-pool allocated. tech_pvt must be passed in, not looked up from the
       channel private - cleanup nulls that before waiting. See CLAUDE.md. */
    struct write_thread_args {
        switch_core_session_t *session;
        private_t *tech_pvt;
    };

    void *SWITCH_THREAD_FUNC write_frame_thread(switch_thread_t *thread, void *obj) {
        auto *args = (write_thread_args *)obj;
        switch_core_session_t *session = args->session;
        private_t *tech_pvt = args->tech_pvt;

        /* Must be the first declaration: it publishes write_thread_done on every
           return below, and (destroyed last) only after the timer and codec are gone. */
        struct done_guard {
            private_t *p;
            ~done_guard() {
                switch_mutex_lock(p->write_mutex);
                p->write_thread_done = 1;
                switch_mutex_unlock(p->write_mutex);
            }
        } guard{tech_pvt};

        switch_channel_t *channel = switch_core_session_get_channel(session);
        if (!channel) return NULL;

        /* Cleanup can win the race against our first instruction. */
        if (tech_pvt->close_requested) {
            switch_log_printf(SWITCH_CHANNEL_SESSION_LOG(session), SWITCH_LOG_DEBUG,
                              "write_frame_thread: close already requested, not starting\n");
            return NULL;
        }

        switch_timer_t timer = {0};
        switch_frame_t write_frame = {0};
        switch_codec_t write_codec = {0};
        switch_codec_t *read_codec;

        uint32_t sample_rate = tech_pvt->sampling;
        uint32_t channels = tech_pvt->channels;

        read_codec = switch_core_session_get_read_codec(session);
        if (!read_codec || !read_codec->implementation) {
            switch_log_printf(SWITCH_CHANNEL_SESSION_LOG(session), SWITCH_LOG_WARNING, "write_frame_thread: no read codec available, shutting down\n");
            return NULL;
        }

        uint32_t interval = read_codec->implementation->microseconds_per_packet / 1000;
        uint32_t samples = switch_samples_per_packet(sample_rate, interval);
        uint32_t tsamples = read_codec->implementation->actual_samples_per_second;
        uint32_t bytes = samples * 2 * channels;

        if (switch_core_codec_init(&write_codec, "L16", NULL, NULL, sample_rate, interval, channels,
                                   SWITCH_CODEC_FLAG_ENCODE | SWITCH_CODEC_FLAG_DECODE, NULL,
                                   switch_core_session_get_pool(session)) != SWITCH_STATUS_SUCCESS) {
            switch_log_printf(SWITCH_CHANNEL_SESSION_LOG(session), SWITCH_LOG_ERROR,
                              "write_frame_thread: Codec Init Failed. Cannot Start Write Thread\n");
            return NULL;
        }
        switch_log_printf(SWITCH_CHANNEL_SESSION_LOG(session), SWITCH_LOG_DEBUG,
                          "Codec Activated L16@%uhz %u channels %dms\n", sample_rate, channels, interval);
        write_frame.codec = &write_codec;
        write_frame.data = switch_core_session_alloc(session, SWITCH_RECOMMENDED_BUFFER_SIZE);
        write_frame.channels = channels;
        write_frame.rate = sample_rate;
        write_frame.buflen = SWITCH_RECOMMENDED_BUFFER_SIZE;

        switch_log_printf(SWITCH_CHANNEL_SESSION_LOG(session), SWITCH_LOG_DEBUG,
                          "started write frame thread with sample rate [%u] interval [%u] samples [%u] tsamples [%u] bytes [%u]\n",
                          sample_rate, interval, samples, tsamples, bytes);

        if (switch_core_timer_init(&timer, "soft", interval, tsamples, NULL) != SWITCH_STATUS_SUCCESS) {
            switch_log_printf(SWITCH_CHANNEL_SESSION_LOG(session), SWITCH_LOG_ERROR, "Timer Setup Failed. Cannot Start Write Thread\n");
            switch_core_codec_destroy(&write_codec);
            return NULL;
        }

        while (!tech_pvt->close_requested && switch_core_session_running(session)) {
            if (tech_pvt->use_write_replace) {
                /* write_sbuffer is drained by stream_write_replace_frame() in
                   the session's own write loop; draining it here too would
                   consume each frame twice and double the playback rate. */
                switch_core_timer_next(&timer);
                continue;
            }
            if (switch_mutex_trylock(tech_pvt->write_mutex) == SWITCH_STATUS_SUCCESS) {
                switch_size_t available = switch_buffer_inuse(tech_pvt->write_sbuffer);
                if (available >= bytes) {
                    write_frame.datalen = (uint32_t)switch_buffer_read(tech_pvt->write_sbuffer, write_frame.data, bytes);
                    write_frame.samples = write_frame.datalen / 2 / channels;
                    switch_status_t wres = SWITCH_STATUS_SUCCESS;
                    /* Required: writing to a dying channel blocks on the session I/O
                       lock that teardown holds, deadlocking cleanup. */
                    if (switch_channel_ready(channel)) {
                        wres = switch_core_session_write_frame(session, &write_frame, SWITCH_IO_FLAG_NONE, 0);
                    }
                }
                switch_mutex_unlock(tech_pvt->write_mutex);
            }
            switch_core_timer_next(&timer);
        }

        switch_core_timer_destroy(&timer);
        switch_core_codec_destroy(&write_codec);
        return NULL;
    }

    switch_status_t stream_data_init(private_t *tech_pvt, switch_core_session_t *session, char *wsUri,
                                     uint32_t sampling, int desiredSampling, int channels, char *metadata, responseHandler_t responseHandler,
                                     int deflate, int heart_beat, bool suppressLog, int rtp_packets, const char* extra_headers,
                                     bool no_reconnect,
                                     const char *tls_cafile, const char *tls_keyfile, const char *tls_certfile,
                                     bool tls_disable_hostname_validation)
    {
        int err; //speex

        switch_memory_pool_t *pool = switch_core_session_get_pool(session);

        memset(tech_pvt, 0, sizeof(private_t));

        strncpy(tech_pvt->sessionId, switch_core_session_get_uuid(session), MAX_SESSION_ID);
        strncpy(tech_pvt->ws_uri, wsUri, MAX_WS_URI);
        tech_pvt->sampling = sampling;
        tech_pvt->wsSampling = desiredSampling;
        tech_pvt->responseHandler = responseHandler;
        tech_pvt->rtp_packets = rtp_packets;
        tech_pvt->channels = channels;
        tech_pvt->audio_paused = 0;

        if (metadata) strncpy(tech_pvt->initialMetadata, metadata, MAX_METADATA_LEN);

        //size_t buflen = (FRAME_SIZE_8000 * desiredSampling / 8000 * channels * 1000 / RTP_PERIOD * BUFFERED_SEC);
        const size_t buflen = (FRAME_SIZE_8000 * desiredSampling / 8000 * channels * rtp_packets);
        
        auto sp = AudioStreamer::create(tech_pvt->sessionId, wsUri, responseHandler, deflate, heart_beat,
                                        suppressLog, extra_headers, no_reconnect,
                                        tls_cafile, tls_keyfile,
                                        tls_certfile, tls_disable_hostname_validation,
                                        /* geminiMode */ false, /* geminiOutputRate */ 24000,
                                        /* geminiKickstart */ nullptr);

        tech_pvt->pAudioStreamer = new std::shared_ptr<AudioStreamer>(sp);

        switch_mutex_init(&tech_pvt->mutex, SWITCH_MUTEX_NESTED, pool);
        switch_mutex_init(&tech_pvt->write_mutex, SWITCH_MUTEX_NESTED, pool);

        if (switch_buffer_create(pool, &tech_pvt->read_sbuffer, buflen) != SWITCH_STATUS_SUCCESS) {
            switch_log_printf(SWITCH_CHANNEL_SESSION_LOG(session), SWITCH_LOG_ERROR,
                "%s: Error creating read switch buffer.\n", tech_pvt->sessionId);
            return SWITCH_STATUS_FALSE;
        }

        if (switch_buffer_create(pool, &tech_pvt->write_sbuffer, buflen) != SWITCH_STATUS_SUCCESS) {
            switch_log_printf(SWITCH_CHANNEL_SESSION_LOG(session), SWITCH_LOG_ERROR,
                "%s: Error creating write switch buffer.\n", tech_pvt->sessionId);
            return SWITCH_STATUS_FALSE;
        }

        if (desiredSampling != sampling) {
            switch_log_printf(SWITCH_CHANNEL_SESSION_LOG(session), SWITCH_LOG_DEBUG, "(%s) resampling from %u to %u\n", tech_pvt->sessionId, sampling, desiredSampling);
            tech_pvt->read_resampler = speex_resampler_init(channels, sampling, desiredSampling, SWITCH_RESAMPLE_QUALITY, &err);
            if (0 != err) {
                switch_log_printf(SWITCH_CHANNEL_SESSION_LOG(session), SWITCH_LOG_ERROR, "Error initializing read resampler: %s.\n", speex_resampler_strerror(err));
                return SWITCH_STATUS_FALSE;
            }
            tech_pvt->write_resampler = speex_resampler_init(channels, desiredSampling, sampling, SWITCH_RESAMPLE_QUALITY, &err);
            if (0 != err) {
                switch_log_printf(SWITCH_CHANNEL_SESSION_LOG(session), SWITCH_LOG_ERROR, "Error initializing write resampler: %s.\n", speex_resampler_strerror(err));
                return SWITCH_STATUS_FALSE;
            }
        }
        else {
            switch_log_printf(SWITCH_CHANNEL_SESSION_LOG(session), SWITCH_LOG_DEBUG, "(%s) no resampling needed for this call\n", tech_pvt->sessionId);
        }

        switch_log_printf(SWITCH_CHANNEL_SESSION_LOG(session), SWITCH_LOG_DEBUG, "(%s) stream_data_init\n", tech_pvt->sessionId);

        return SWITCH_STATUS_SUCCESS;
    }

    void destroy_tech_pvt(private_t* tech_pvt) {
        switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_INFO, "%s destroy_tech_pvt\n", tech_pvt->sessionId);
        if (tech_pvt->read_resampler) {
            speex_resampler_destroy(tech_pvt->read_resampler);
            tech_pvt->read_resampler = nullptr;
        }
        if (tech_pvt->write_resampler) {
            speex_resampler_destroy(tech_pvt->write_resampler);
            tech_pvt->write_resampler = nullptr;
        }
        if (tech_pvt->mutex) {
            switch_mutex_destroy(tech_pvt->mutex);
            tech_pvt->mutex = nullptr;
        }
        if (tech_pvt->write_mutex) {
            switch_mutex_destroy(tech_pvt->write_mutex);
            tech_pvt->write_mutex = nullptr;
        }
        if (tech_pvt->read_sbuffer) {
            switch_buffer_destroy(&tech_pvt->read_sbuffer);
            tech_pvt->read_sbuffer = nullptr;
        }
        if (tech_pvt->write_sbuffer) {
            switch_buffer_destroy(&tech_pvt->write_sbuffer);
            tech_pvt->write_sbuffer = nullptr;
        }
    }

}

extern "C" {
    int validate_ws_uri(const char* url, char* wsUri) {
        const char* scheme = nullptr;
        const char* hostStart = nullptr;
        const char* hostEnd = nullptr;
        const char* portStart = nullptr;

        // Check scheme
        if (strncmp(url, "ws://", 5) == 0) {
            scheme = "ws";
            hostStart = url + 5;
        } else if (strncmp(url, "wss://", 6) == 0) {
            scheme = "wss";
            hostStart = url + 6;
        } else {
            return 0;
        }

        // Find host end or port start
        hostEnd = hostStart;
        while (*hostEnd && *hostEnd != ':' && *hostEnd != '/') {
            if (!std::isalnum(*hostEnd) && *hostEnd != '-' && *hostEnd != '.') {
                return 0;
            }
            ++hostEnd;
        }

        // Check if host is empty
        if (hostStart == hostEnd) {
            return 0;
        }

        // Check for port
        if (*hostEnd == ':') {
            portStart = hostEnd + 1;
            while (*portStart && *portStart != '/') {
                if (!std::isdigit(*portStart)) {
                    return 0;
                }
                ++portStart;
            }
        }

        // Copy valid URI to wsUri
        std::strncpy(wsUri, url, MAX_WS_URI);
        return 1;
    }

    switch_status_t is_valid_utf8(const char *str) {
        switch_status_t status = SWITCH_STATUS_FALSE;
        while (*str) {
            if ((*str & 0x80) == 0x00) {
                // 1-byte character
                str++;
            } else if ((*str & 0xE0) == 0xC0) {
                // 2-byte character
                if ((str[1] & 0xC0) != 0x80) {
                    return status;
                }
                str += 2;
            } else if ((*str & 0xF0) == 0xE0) {
                // 3-byte character
                if ((str[1] & 0xC0) != 0x80 || (str[2] & 0xC0) != 0x80) {
                    return status;
                }
                str += 3;
            } else if ((*str & 0xF8) == 0xF0) {
                // 4-byte character
                if ((str[1] & 0xC0) != 0x80 || (str[2] & 0xC0) != 0x80 || (str[3] & 0xC0) != 0x80) {
                    return status;
                }
                str += 4;
            } else {
                // invalid character
                return status;
            }
        }
        return SWITCH_STATUS_SUCCESS;
    }

    switch_status_t stream_session_send_text(switch_core_session_t *session, char* text) {
        switch_channel_t *channel = switch_core_session_get_channel(session);
        auto *bug = (switch_media_bug_t*) switch_channel_get_private(channel, MY_BUG_NAME);
        if (!bug) {
            switch_log_printf(SWITCH_CHANNEL_SESSION_LOG(session), SWITCH_LOG_ERROR, "stream_session_send_text failed because no bug\n");
            return SWITCH_STATUS_FALSE;
        }
        auto *tech_pvt = (private_t*) switch_core_media_bug_get_user_data(bug);

        if (!tech_pvt) return SWITCH_STATUS_FALSE;

        std::shared_ptr<AudioStreamer> streamer;

        switch_mutex_lock(tech_pvt->mutex);

        if (tech_pvt->pAudioStreamer) {
            auto sp_wrap = static_cast<std::shared_ptr<AudioStreamer>*>(tech_pvt->pAudioStreamer);
            if (sp_wrap && *sp_wrap) {
                streamer = *sp_wrap; // copy shared_ptr
            }
        }

        switch_mutex_unlock(tech_pvt->mutex);

        if (streamer) {
            streamer->writeText(text);
            return SWITCH_STATUS_SUCCESS;
        }

        return SWITCH_STATUS_FALSE;
    }

    switch_status_t stream_session_pauseresume(switch_core_session_t *session, int pause) {
        switch_channel_t *channel = switch_core_session_get_channel(session);
        auto *bug = (switch_media_bug_t*) switch_channel_get_private(channel, MY_BUG_NAME);
        if (!bug) {
            switch_log_printf(SWITCH_CHANNEL_SESSION_LOG(session), SWITCH_LOG_ERROR, "stream_session_pauseresume failed because no bug\n");
            return SWITCH_STATUS_FALSE;
        }
        auto *tech_pvt = (private_t*) switch_core_media_bug_get_user_data(bug);

        if (!tech_pvt) return SWITCH_STATUS_FALSE;

        switch_core_media_bug_flush(bug);
        tech_pvt->audio_paused = pause;
        return SWITCH_STATUS_SUCCESS;
    }

    switch_status_t stream_session_flush(switch_core_session_t *session) {
        switch_channel_t *channel = switch_core_session_get_channel(session);
        auto *bug = (switch_media_bug_t*) switch_channel_get_private(channel, MY_BUG_NAME);
        if (!bug) {
            switch_log_printf(SWITCH_CHANNEL_SESSION_LOG(session), SWITCH_LOG_ERROR, "stream_session_flush failed because no bug\n");
            return SWITCH_STATUS_FALSE;
        }
        auto *tech_pvt = (private_t*) switch_core_media_bug_get_user_data(bug);
        if (!tech_pvt) return SWITCH_STATUS_FALSE;

        // 1. Zero the write buffer (audio already decoded and waiting to play).
        // Commenting out for a moment
        // switch_mutex_lock(tech_pvt->write_mutex);
        // switch_buffer_zero(tech_pvt->write_sbuffer);
        // switch_mutex_unlock(tech_pvt->write_mutex);

        // 2. Discard audio still in transit from the WebSocket:
        //    - signals the event thread to drain the input evbuffer
        //      (TCP bytes received but not yet parsed)
        //    - sets a discard flag that silently drops any raw audio frames
        //      arriving after this point until the server sends a non-audio
        //      message (indicating the interrupted response has ended).
        std::shared_ptr<AudioStreamer> streamer;
        switch_mutex_lock(tech_pvt->mutex);
        if (tech_pvt->pAudioStreamer) {
            auto* sp_wrap = static_cast<std::shared_ptr<AudioStreamer>*>(tech_pvt->pAudioStreamer);
            if (sp_wrap && *sp_wrap) streamer = *sp_wrap;
        }
        switch_mutex_unlock(tech_pvt->mutex);

        if (streamer) {
            streamer->discardIncomingAudio();
        }

        switch_log_printf(SWITCH_CHANNEL_SESSION_LOG(session), SWITCH_LOG_INFO,
                          "stream_session_flush: write buffer cleared, incoming audio discarded\n");
        return SWITCH_STATUS_SUCCESS;
    }

    switch_status_t stream_session_init(switch_core_session_t *session,
                                        responseHandler_t responseHandler,
                                        uint32_t samples_per_second,
                                        char *wsUri,
                                        int sampling,
                                        int channels,
                                        char* metadata,
                                        void **ppUserData)
    {
        int deflate, heart_beat;
        bool suppressLog = false;
        bool no_reconnect = false;
        const char* buffer_size;
        const char* extra_headers;
        int rtp_packets = 1; //20ms burst
        const char* tls_cafile = NULL;
        const char* tls_keyfile = NULL;
        const char* tls_certfile = NULL;
        bool tls_disable_hostname_validation = false;

        switch_channel_t *channel = switch_core_session_get_channel(session);

        if (switch_channel_var_true(channel, "STREAM_MESSAGE_DEFLATE")) {
            deflate = 1;
        }

        if (switch_channel_var_true(channel, "STREAM_SUPPRESS_LOG")) {
            suppressLog = true;
        }

        if (switch_channel_var_true(channel, "STREAM_NO_RECONNECT")) {
            no_reconnect = true;
        }

        tls_cafile = switch_channel_get_variable(channel, "STREAM_TLS_CA_FILE");
        tls_keyfile = switch_channel_get_variable(channel, "STREAM_TLS_KEY_FILE");
        tls_certfile = switch_channel_get_variable(channel, "STREAM_TLS_CERT_FILE");

        if (switch_channel_var_true(channel, "STREAM_TLS_DISABLE_HOSTNAME_VALIDATION")) {
            tls_disable_hostname_validation = true;
        }

        const char* heartBeat = switch_channel_get_variable(channel, "STREAM_HEART_BEAT");
        if (heartBeat) {
            char *endptr;
            long value = strtol(heartBeat, &endptr, 10);
            if (*endptr == '\0' && value <= INT_MAX && value >= INT_MIN) {
                heart_beat = (int) value;
            }
        }

        if ((buffer_size = switch_channel_get_variable(channel, "STREAM_BUFFER_SIZE"))) {
            int bSize = atoi(buffer_size);
            if(bSize % 20 != 0) {
                switch_log_printf(SWITCH_CHANNEL_SESSION_LOG(session), SWITCH_LOG_WARNING, "%s: Buffer size of %s is not a multiple of 20ms. Using default 20ms.\n",
                                  switch_channel_get_name(channel), buffer_size);
            } else if(bSize >= 20){
                rtp_packets = bSize/20;
            }
        }

        extra_headers = switch_channel_get_variable(channel, "STREAM_EXTRA_HEADERS");

        // allocate per-session tech_pvt
        auto* tech_pvt = (private_t *) switch_core_session_alloc(session, sizeof(private_t));

        if (!tech_pvt) {
            switch_log_printf(SWITCH_CHANNEL_SESSION_LOG(session), SWITCH_LOG_ERROR, "error allocating memory!\n");
            return SWITCH_STATUS_FALSE;
        }
        if (SWITCH_STATUS_SUCCESS != stream_data_init(tech_pvt, session, wsUri, samples_per_second, sampling, channels,
                                                        metadata, responseHandler, deflate, heart_beat, suppressLog, rtp_packets,
                                                        extra_headers, no_reconnect, tls_cafile, tls_keyfile, tls_certfile, tls_disable_hostname_validation)) {
            destroy_tech_pvt(tech_pvt);
            return SWITCH_STATUS_FALSE;
        }

        /* Mono/mixed: deliver injected audio through the session's write loop
           (SMBF_WRITE_REPLACE hook set in start_capture), never via the
           write thread. Stereo falls back to the write thread path. */
        if (channels == 1) {
            tech_pvt->use_write_replace = 1;
            tech_pvt->inject_scratch = (uint8_t *)switch_core_session_alloc(session, SWITCH_RECOMMENDED_BUFFER_SIZE);
            tech_pvt->inject_scratch_size = SWITCH_RECOMMENDED_BUFFER_SIZE;
        }

        *ppUserData = tech_pvt;

        return SWITCH_STATUS_SUCCESS;
    }

    switch_status_t stream_session_write_thread_init(switch_core_session_t *session, void *pUserData) {
        private_t *tech_pvt = (private_t *)pUserData;
        switch_memory_pool_t *pool = switch_core_session_get_pool(session);
        switch_threadattr_t *thd_attr = NULL;
        switch_status_t status;

        auto *args = (write_thread_args *)switch_core_session_alloc(session, sizeof(write_thread_args));
        if (!args) {
            switch_log_printf(SWITCH_CHANNEL_SESSION_LOG(session), SWITCH_LOG_ERROR,
                              "(%s) error allocating write thread args\n", tech_pvt->sessionId);
            tech_pvt->write_thread = nullptr;
            tech_pvt->write_thread_done = 1;
            return SWITCH_STATUS_FALSE;
        }
        args->session = session;
        args->tech_pvt = tech_pvt;

        switch_threadattr_create(&thd_attr, pool);
        /* Detached, NOT joinable: cleanup cannot join on the hangup path, and an
           unjoined joinable thread leaks its stack mapping. See CLAUDE.md. */
        switch_threadattr_detach_set(thd_attr, 1);
        switch_threadattr_stacksize_set(thd_attr, SWITCH_THREAD_STACKSIZE);
        tech_pvt->write_thread_done = 0;

        status = switch_thread_create(&tech_pvt->write_thread, thd_attr, write_frame_thread, args, pool);
        if (status != SWITCH_STATUS_SUCCESS) {
            /* apr_thread_create() leaves the handle set on failure; clearing it keeps
               cleanup from waiting out the timeout for a thread that never ran. */
            tech_pvt->write_thread = nullptr;
            tech_pvt->write_thread_done = 1;
            switch_log_printf(SWITCH_CHANNEL_SESSION_LOG(session), SWITCH_LOG_ERROR,
                              "(%s) failed to create write frame thread (%d)\n", tech_pvt->sessionId, status);
            return SWITCH_STATUS_FALSE;
        }
        return SWITCH_STATUS_SUCCESS;
    }

    switch_bool_t stream_frame(switch_media_bug_t *bug) {
        auto *tech_pvt = (private_t *)switch_core_media_bug_get_user_data(bug);
        if (!tech_pvt) return SWITCH_TRUE;
        if (tech_pvt->audio_paused || tech_pvt->cleanup_started) return SWITCH_TRUE;
        
        std::shared_ptr<AudioStreamer> streamer;
        std::vector<std::vector<uint8_t>> pending_send;

        if (switch_mutex_trylock(tech_pvt->mutex) != SWITCH_STATUS_SUCCESS) {
            return SWITCH_TRUE;
        }

        if (!tech_pvt->pAudioStreamer) {
            switch_mutex_unlock(tech_pvt->mutex);
            return SWITCH_TRUE;
        }

        auto sp_ptr = static_cast<std::shared_ptr<AudioStreamer>*>(tech_pvt->pAudioStreamer);
        if (!sp_ptr || !(*sp_ptr)) {
            switch_mutex_unlock(tech_pvt->mutex);
            return SWITCH_TRUE;
        }

        streamer = *sp_ptr;

        auto *resampler = tech_pvt->read_resampler;
        const int channels = tech_pvt->channels;
        const int rtp_packets = tech_pvt->rtp_packets;

        if (nullptr == resampler) {
            
            uint8_t data_buf[SWITCH_RECOMMENDED_BUFFER_SIZE];
            switch_frame_t frame = {};
            frame.data = data_buf;
            frame.buflen = SWITCH_RECOMMENDED_BUFFER_SIZE;

            while (switch_core_media_bug_read(bug, &frame, SWITCH_TRUE) == SWITCH_STATUS_SUCCESS) {
                if (!frame.datalen) {
                    continue;
                }

                if (rtp_packets == 1) {
                    pending_send.emplace_back((uint8_t*)frame.data, (uint8_t*)frame.data + frame.datalen);
                    continue;
                }

                size_t freespace = switch_buffer_freespace(tech_pvt->read_sbuffer);
                
                if (freespace >= frame.datalen) {
                    switch_buffer_write(tech_pvt->read_sbuffer, static_cast<uint8_t *>(frame.data), frame.datalen);
                }

                if (switch_buffer_freespace(tech_pvt->read_sbuffer) == 0) {
                    switch_size_t inuse = switch_buffer_inuse(tech_pvt->read_sbuffer);
                    if (inuse > 0) {
                        std::vector<uint8_t> tmp(inuse);
                        switch_buffer_read(tech_pvt->read_sbuffer, tmp.data(), inuse);
                        switch_buffer_zero(tech_pvt->read_sbuffer);
                        pending_send.emplace_back(std::move(tmp));
                    }
                }
            }
            
        } else {

            uint8_t data[SWITCH_RECOMMENDED_BUFFER_SIZE];
            switch_frame_t frame = {};
            frame.data = data;
            frame.buflen = SWITCH_RECOMMENDED_BUFFER_SIZE;

            while (switch_core_media_bug_read(bug, &frame, SWITCH_TRUE) == SWITCH_STATUS_SUCCESS) {
                if(!frame.datalen) {
                    continue;
                }

                const size_t freespace = switch_buffer_freespace(tech_pvt->read_sbuffer);
                spx_uint32_t in_len = frame.samples;
                spx_uint32_t out_len = (freespace / (tech_pvt->channels * sizeof(spx_int16_t)));
                
                if(out_len == 0) {
                    if(freespace == 0) {
                        switch_size_t inuse = switch_buffer_inuse(tech_pvt->read_sbuffer);
                        if (inuse > 0) {
                            std::vector<uint8_t> tmp(inuse);
                            switch_buffer_read(tech_pvt->read_sbuffer, tmp.data(), inuse);
                            switch_buffer_zero(tech_pvt->read_sbuffer);
                            pending_send.emplace_back(std::move(tmp));
                        }
                    }
                    continue;
                }

                std::vector<spx_int16_t> out;
                out.resize((size_t)out_len * (size_t)channels);

                if(channels == 1) {
                    speex_resampler_process_int(resampler,
                                    0,
                                    (const spx_int16_t *)frame.data,
                                    &in_len,
                                    out.data(),
                                    &out_len);
                } else {
                    speex_resampler_process_interleaved_int(resampler,
                                    (const spx_int16_t *)frame.data,
                                    &in_len,
                                    out.data(),
                                    &out_len);
                }

                if(out_len > 0) {
                    const size_t bytes_written = (size_t)out_len * (size_t)channels * sizeof(spx_int16_t);

                    if (rtp_packets == 1) { //20ms packet
                        const uint8_t* p = (const uint8_t*)out.data();
                        pending_send.emplace_back(p, p + bytes_written);
                        continue;
                    }

                    if (bytes_written <= switch_buffer_freespace(tech_pvt->read_sbuffer)) {
                        switch_buffer_write(tech_pvt->read_sbuffer, (const uint8_t *)out.data(), bytes_written);
                    }
                }

                if (switch_buffer_freespace(tech_pvt->read_sbuffer) == 0) {
                    switch_size_t inuse = switch_buffer_inuse(tech_pvt->read_sbuffer);
                    if (inuse > 0) {
                        std::vector<uint8_t> tmp(inuse);
                        switch_buffer_read(tech_pvt->read_sbuffer, tmp.data(), inuse);
                        switch_buffer_zero(tech_pvt->read_sbuffer);
                        pending_send.emplace_back(std::move(tmp));
                    }
                }
            }
        }
        
        switch_mutex_unlock(tech_pvt->mutex);
    
        if (!streamer || !streamer->isConnected()) return SWITCH_TRUE;

        for (auto &chunk : pending_send) {
            if (!chunk.empty()) {
                streamer->writeBinary(chunk.data(), chunk.size());
            }
        }

        return SWITCH_TRUE;
    }

    switch_bool_t stream_write_replace_frame(switch_media_bug_t *bug) {
        switch_core_session_t *session = switch_core_media_bug_get_session(bug);
        auto *tech_pvt = (private_t *)switch_core_media_bug_get_user_data(bug);
        if (!tech_pvt || tech_pvt->close_requested || tech_pvt->cleanup_started) return SWITCH_TRUE;

        switch_frame_t *frame = switch_core_media_bug_get_write_replace_frame(bug);
        if (!frame || !frame->datalen) return SWITCH_TRUE;

        const size_t bytes = frame->datalen;
        if (!tech_pvt->inject_scratch || tech_pvt->inject_scratch_size < bytes) return SWITCH_TRUE;

        size_t consumed = 0;
        if (switch_mutex_trylock(tech_pvt->write_mutex) == SWITCH_STATUS_SUCCESS) {
            if (switch_buffer_inuse(tech_pvt->write_sbuffer) >= bytes) {
                consumed = switch_buffer_read(tech_pvt->write_sbuffer, tech_pvt->inject_scratch, bytes);
            }
            switch_mutex_unlock(tech_pvt->write_mutex);
        }

        if (consumed == bytes) {
            /* Substitute only the data; keep the frame's codec/size so the
               write-bug loop still hits the perfect-encode path. */
            frame->data = tech_pvt->inject_scratch;
            frame->datalen = (uint32_t)bytes;
            frame->samples = (uint32_t)(bytes / 2 / tech_pvt->channels);
        }

        return SWITCH_TRUE;
    }

    switch_status_t stream_session_abort(void *pUserData) {
        auto *tech_pvt = (private_t *)pUserData;
        if (!tech_pvt) return SWITCH_STATUS_FALSE;

        if (tech_pvt->pAudioStreamer) {
            auto *sp_wrap = static_cast<std::shared_ptr<AudioStreamer>*>(tech_pvt->pAudioStreamer);
            if (sp_wrap) {
                if (*sp_wrap) {
                    (*sp_wrap)->markCleanedUp();
                    (*sp_wrap)->disconnect();
                }
                delete sp_wrap;
            }
            tech_pvt->pAudioStreamer = nullptr;
        }
        destroy_tech_pvt(tech_pvt);
        return SWITCH_STATUS_SUCCESS;
    }

    switch_status_t stream_session_cleanup(switch_core_session_t *session, char* text, int channelIsClosing) {
        switch_channel_t *channel = switch_core_session_get_channel(session);
        auto *bug = (switch_media_bug_t*) switch_channel_get_private(channel, MY_BUG_NAME);
        if(bug)
        {
            auto* tech_pvt = (private_t*) switch_core_media_bug_get_user_data(bug);
            char sessionId[MAX_SESSION_ID];
            strcpy(sessionId, tech_pvt->sessionId);

            std::shared_ptr<AudioStreamer>* sp_wrap = nullptr;
            std::shared_ptr<AudioStreamer> streamer;
            switch_thread_t *write_thread = nullptr;
            int write_thread_exited = 1; /* no thread to wait for -> full cleanup */

            switch_mutex_lock(tech_pvt->mutex);

            if (tech_pvt->cleanup_started) {
                switch_mutex_unlock(tech_pvt->mutex);
                return SWITCH_STATUS_SUCCESS;
            }
            tech_pvt->cleanup_started = 1;
            tech_pvt->close_requested = 1;

            switch_log_printf(SWITCH_CHANNEL_SESSION_LOG(session), SWITCH_LOG_DEBUG, "(%s) stream_session_cleanup\n", sessionId);

            switch_channel_set_private(channel, MY_BUG_NAME, nullptr);

            sp_wrap = static_cast<std::shared_ptr<AudioStreamer>*>(tech_pvt->pAudioStreamer);
            tech_pvt->pAudioStreamer = nullptr;

            if (sp_wrap && *sp_wrap) {
                streamer = *sp_wrap;
            }

            write_thread = tech_pvt->write_thread;
            tech_pvt->write_thread = nullptr;

            switch_mutex_unlock(tech_pvt->mutex);

            if (!channelIsClosing) {
                switch_core_media_bug_remove(session, &bug);
            }

            if (sp_wrap) {
                delete sp_wrap;
                sp_wrap = nullptr;
            }

            if(streamer) {
                streamer->deleteFiles();
                if (text) streamer->writeText(text);

                /* Nulls all callbacks, so no websocket event can reach session
                   context after this point. */
                streamer->markCleanedUp();

                if (!channelIsClosing) {
                    /* Not in the teardown path - safe to block on the close handshake. */
                    streamer->disconnect();
                } else {
                    /* disconnect() blocks on the close handshake and a dead backend can
                       stall it indefinitely, so hand it to a detached thread; the moved
                       shared_ptr keeps the AudioStreamer alive until it finishes.
                       The catch is load-bearing, not style: we unwind into a C frame
                       (switch_core_media_bug_close), so an escaping exception would
                       std::terminate all of FreeSWITCH. See CLAUDE.md. */
                    try {
                        std::thread([s = std::move(streamer)]() mutable {
                            s->disconnect();
                        }).detach();
                    } catch (const std::exception &e) {
                        /* streamer died with the closure; no synchronous fallback -
                           that would block teardown, which is what we are avoiding. */
                        switch_log_printf(SWITCH_CHANNEL_SESSION_LOG(session), SWITCH_LOG_ERROR,
                                          "(%s) stream_session_cleanup: could not spawn disconnect "
                                          "thread (%s); closing without handshake\n", sessionId, e.what());
                    } catch (...) {
                        switch_log_printf(SWITCH_CHANNEL_SESSION_LOG(session), SWITCH_LOG_ERROR,
                                          "(%s) stream_session_cleanup: could not spawn disconnect "
                                          "thread; closing without handshake\n", sessionId);
                    }
                }
            }

            if (write_thread) {
                if (!channelIsClosing) {
                    /* destroy_tech_pvt() below frees write_mutex and write_sbuffer, so
                       the thread must be out of its loop first. It is detached, so no
                       join - wait on write_thread_done (normally ~20 ms). */
                    int waited_ms = 0;
                    for (;;) {
                        switch_mutex_lock(tech_pvt->write_mutex);
                        write_thread_exited = tech_pvt->write_thread_done;
                        switch_mutex_unlock(tech_pvt->write_mutex);
                        if (write_thread_exited || waited_ms >= WRITE_THREAD_EXIT_TIMEOUT_MS) break;
                        switch_yield(5000); /* 5 ms */
                        waited_ms += 5;
                    }
                    if (!write_thread_exited) {
                        switch_log_printf(SWITCH_CHANNEL_SESSION_LOG(session), SWITCH_LOG_WARNING,
                                          "(%s) stream_session_cleanup: write thread did not report exit within "
                                          "%d ms; skipping destroy_tech_pvt to avoid use-after-free\n",
                                          sessionId, WRITE_THREAD_EXIT_TIMEOUT_MS);
                    }
                } else {
                    /* Cannot wait here: we are inside SWITCH_ABC_TYPE_CLOSE and the
                       teardown holds the session I/O lock. The detached thread self-exits
                       within a timer tick and needs no reclaiming.
                       Best-effort only - nothing orders that exit against the session pool
                       being freed. See CLAUDE.md. */
                    switch_log_printf(SWITCH_CHANNEL_SESSION_LOG(session), SWITCH_LOG_DEBUG,
                                      "(%s) stream_session_cleanup: not waiting for write thread on channel close "
                                      "(close_requested set, detached thread will self-exit)\n", sessionId);
                }
            }

            if (!channelIsClosing && write_thread_exited) {
                destroy_tech_pvt(tech_pvt);
            } else {
                /* Write thread may still be live (hangup path, or the wait timed out).
                   Leave write_mutex/write_sbuffer to the session pool, but the speex
                   resamplers are malloc'd - the pool never reclaims them, and the write
                   thread never touches them, so free them here. */
                if (tech_pvt->read_resampler) {
                    speex_resampler_destroy(tech_pvt->read_resampler);
                    tech_pvt->read_resampler = nullptr;
                }
                if (tech_pvt->write_resampler) {
                    speex_resampler_destroy(tech_pvt->write_resampler);
                    tech_pvt->write_resampler = nullptr;
                }
            }

            switch_log_printf(SWITCH_CHANNEL_SESSION_LOG(session), SWITCH_LOG_INFO, "(%s) stream_session_cleanup: connection closed\n", sessionId);
            return SWITCH_STATUS_SUCCESS;
        }

        switch_log_printf(SWITCH_CHANNEL_SESSION_LOG(session), SWITCH_LOG_DEBUG, "stream_session_cleanup: no bug - websocket connection already closed\n");
        return SWITCH_STATUS_FALSE;
    }
}
