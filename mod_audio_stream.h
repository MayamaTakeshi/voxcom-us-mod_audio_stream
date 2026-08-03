#ifndef MOD_AUDIO_STREAM_H
#define MOD_AUDIO_STREAM_H

#include <switch.h>
#include <speex/speex_resampler.h>

#define MY_BUG_NAME "audio_stream"
#define MAX_SESSION_ID (256)
#define MAX_WS_URI (4096)
#define MAX_METADATA_LEN (8192)

/* Cap on how long stream_session_cleanup() waits for write_frame_thread to exit
   (normally ~20 ms, one timer tick). */
#define WRITE_THREAD_EXIT_TIMEOUT_MS (2000)

#define EVENT_CONNECT "mod_audio_stream::connect"
#define EVENT_DISCONNECT "mod_audio_stream::disconnect"
#define EVENT_ERROR "mod_audio_stream::error"
#define EVENT_JSON "mod_audio_stream::json"
#define EVENT_PLAY "mod_audio_stream::play"

typedef void (*responseHandler_t)(switch_core_session_t *session, const char *eventName, const char *json);

struct private_data
{
    switch_mutex_t *mutex;
    char sessionId[MAX_SESSION_ID];
    SpeexResamplerState *read_resampler;
    SpeexResamplerState *write_resampler;
    responseHandler_t responseHandler;
    void *pAudioStreamer;
    char ws_uri[MAX_WS_URI];
    int sampling;
    int wsSampling;
    int channels;
    int audio_paused : 1;
    int close_requested : 1;
    int cleanup_started : 1;
    char initialMetadata[8192];
    switch_buffer_t *read_sbuffer;
    switch_buffer_t *write_sbuffer;
    switch_mutex_t *write_mutex;
    switch_thread_t *write_thread;
    /* Set by write_frame_thread on exit, read by cleanup; always under write_mutex.
       Not a bitfield - those share a storage unit, so writes would race. */
    int write_thread_done;
    int rtp_packets;
};

typedef struct private_data private_t;

enum notifyEvent_t
{
    CONNECT_SUCCESS,
    CONNECT_ERROR,
    CONNECTION_DROPPED,
    MESSAGE
};

#endif // MOD_AUDIO_STREAM_H
