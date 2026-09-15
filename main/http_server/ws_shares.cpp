#include <atomic>
#include <string.h>
#include <string>

#include "freertos/FreeRTOS.h"
#include "freertos/queue.h"
#include "freertos/task.h"

#include "esp_log.h"

#include "ArduinoJson.h"

#include "global_state.h"
#include "http_cors.h"
#include "http_utils.h"
#include "macros.h"
#include "psram_allocator.h"
#include "utils.h"
#include "ws_shares.h"

static const char *TAG = "ws_shares";

// A nonce every ~1.8 s at stock settings, so 64 slots is a lot of headroom.
// It only fills up if the browser stalls, and then dropping is the right call.
#define WS_SHARES_QUEUE_LEN 64

// Wait this long after the first event before sending, so a burst leaves as a
// single frame instead of one packet per event.
#define WS_SHARES_COALESCE_MS 100

// Longest frame we accept from a client. Nothing is expected, but the payload
// has to be drained or the next header would be read out of sync.
#define WS_SHARES_MAX_RX_LEN 256

enum
{
    WS_SHARE_NONCE = 0,
    WS_SHARE_VERDICT = 1,
    WS_SHARE_RESET = 2,
};

typedef struct
{
    uint8_t type;
    uint8_t pool;
    int8_t asic_nr;
    bool duplicate;
    bool accepted;
    uint8_t hash[32]; // internal little endian order
    double diff;
    uint64_t ts_ms;
    uint32_t pool_diff;
    uint32_t asic_diff;
    uint32_t nbits;
    uint32_t ntime;
    uint32_t nonce;
    uint32_t version;
    uint32_t count;
    int32_t submit_id;
    char jobid[16];
    char extranonce2[20];
    char reason[32];
} ws_share_event_t;

static QueueHandle_t s_queue = NULL;
static StaticQueue_t s_queue_struct;
static uint8_t *s_queue_storage = NULL;

static int s_client_fds[WS_SHARES_MAX_CLIENTS];
static portMUX_TYPE s_clients_mux = portMUX_INITIALIZER_UNLOCKED;

// read by the producers outside the spinlock as a cheap "is anybody watching"
// check, so it has to be atomic rather than a plain int
static std::atomic<int> s_client_count{0};
static std::atomic<uint32_t> s_dropped{0};

extern httpd_handle_t http_server;

//--------------------------------------------------------------------
// client registry
//--------------------------------------------------------------------

static bool add_client(int fd)
{
    bool added = false;
    portENTER_CRITICAL(&s_clients_mux);
    for (int i = 0; i < WS_SHARES_MAX_CLIENTS; i++) {
        if (s_client_fds[i] == fd) {
            // same socket handed to us twice, nothing to do
            added = true;
            break;
        }
        if (s_client_fds[i] < 0) {
            s_client_fds[i] = fd;
            s_client_count.fetch_add(1, std::memory_order_relaxed);
            added = true;
            break;
        }
    }
    portEXIT_CRITICAL(&s_clients_mux);
    return added;
}

static void remove_client(int fd)
{
    bool removed = false;
    portENTER_CRITICAL(&s_clients_mux);
    for (int i = 0; i < WS_SHARES_MAX_CLIENTS; i++) {
        if (s_client_fds[i] == fd) {
            s_client_fds[i] = -1;
            s_client_count.fetch_sub(1, std::memory_order_relaxed);
            removed = true;
            break;
        }
    }
    portEXIT_CRITICAL(&s_clients_mux);

    if (removed) {
        ESP_LOGI(TAG, "share stream client left (fd %d, %d left)", fd, s_client_count.load(std::memory_order_relaxed));
    }
}

void ws_shares_on_socket_closed(int sockfd)
{
    remove_client(sockfd);
}

//--------------------------------------------------------------------
// producers - POD into the queue, never any I/O
//--------------------------------------------------------------------

static inline void push(ws_share_event_t *ev)
{
    ev->ts_ms = now_ms();

    if (xQueueSendToBack(s_queue, ev, (TickType_t) 0) != pdPASS) {
        s_dropped.fetch_add(1, std::memory_order_relaxed);
    }
}

void ws_shares_push_nonce(const uint8_t hash[32], double diff, uint32_t pool_diff, uint32_t asic_diff, uint32_t nbits,
                          const char *jobid, const char *extranonce2, uint32_t ntime, uint32_t nonce,
                          uint32_t version_rolled, int pool, int asic_nr, bool duplicate, int submit_id)
{
    if (!s_queue || !s_client_count.load(std::memory_order_relaxed)) {
        return;
    }

    ws_share_event_t ev;
    memset(&ev, 0, sizeof(ev));

    ev.type = WS_SHARE_NONCE;
    ev.pool = (uint8_t) (pool & 1);
    ev.asic_nr = (int8_t) asic_nr;
    ev.duplicate = duplicate;
    memcpy(ev.hash, hash, sizeof(ev.hash));
    ev.diff = diff;
    ev.pool_diff = pool_diff;
    ev.asic_diff = asic_diff;
    ev.nbits = nbits;
    ev.ntime = ntime;
    ev.nonce = nonce;
    ev.version = version_rolled;
    ev.submit_id = submit_id;
    if (jobid) {
        strlcpy(ev.jobid, jobid, sizeof(ev.jobid));
    }
    if (extranonce2) {
        strlcpy(ev.extranonce2, extranonce2, sizeof(ev.extranonce2));
    }

    push(&ev);
}

void ws_shares_push_verdict(int pool, int64_t submit_id, bool accepted, const char *reason, uint32_t count)
{
    if (!s_queue || !s_client_count.load(std::memory_order_relaxed)) {
        return;
    }

    ws_share_event_t ev;
    memset(&ev, 0, sizeof(ev));

    ev.type = WS_SHARE_VERDICT;
    ev.pool = (uint8_t) (pool & 1);
    ev.accepted = accepted;
    ev.submit_id = (int32_t) submit_id;
    ev.count = count;
    if (reason) {
        strlcpy(ev.reason, reason, sizeof(ev.reason));
    }

    push(&ev);
}

void ws_shares_push_reset(int pool)
{
    if (!s_queue || !s_client_count.load(std::memory_order_relaxed)) {
        return;
    }

    ws_share_event_t ev;
    memset(&ev, 0, sizeof(ev));

    ev.type = WS_SHARE_RESET;
    ev.pool = (uint8_t) (pool & 1);

    push(&ev);
}

//--------------------------------------------------------------------
// consumer
//--------------------------------------------------------------------

static void hash_to_bitcoin_hex(const uint8_t hash[32], char out[65])
{
    static const char *digits = "0123456789abcdef";

    // the hash is compared as a little endian integer, so the leading zeros
    // everybody looks at live at the end of the buffer
    for (int i = 0; i < 32; i++) {
        uint8_t b = hash[31 - i];
        out[i * 2] = digits[b >> 4];
        out[i * 2 + 1] = digits[b & 0x0f];
    }
    out[64] = 0;
}

static void serialize_event(const ws_share_event_t *ev, JsonArray &arr)
{
    JsonObject o = arr.add<JsonObject>();
    o["ts"] = ev->ts_ms;
    o["p"] = ev->pool;

    switch (ev->type) {
    case WS_SHARE_NONCE: {
        char hex[65];
        hash_to_bitcoin_hex(ev->hash, hex);

        o["t"] = "n";
        o["h"] = hex;
        o["d"] = ev->diff;
        o["pd"] = ev->pool_diff;
        o["ad"] = ev->asic_diff;
        o["nb"] = ev->nbits;
        o["nt"] = ev->ntime;
        o["no"] = ev->nonce;
        o["v"] = ev->version;
        o["job"] = ev->jobid;
        o["en2"] = ev->extranonce2;
        o["a"] = ev->asic_nr;
        o["dup"] = ev->duplicate;
        o["sid"] = ev->submit_id;
        break;
    }
    case WS_SHARE_VERDICT: {
        o["t"] = "v";
        o["sid"] = ev->submit_id;
        o["ok"] = ev->accepted;
        if (ev->count != 1) {
            o["cnt"] = ev->count;
        }
        if (ev->reason[0]) {
            o["why"] = ev->reason;
        }
        break;
    }
    case WS_SHARE_RESET: {
        o["t"] = "r";
        break;
    }
    default:
        break;
    }
}

static void send_to_clients(const std::string &payload)
{
    int fds[WS_SHARES_MAX_CLIENTS];

    portENTER_CRITICAL(&s_clients_mux);
    memcpy(fds, s_client_fds, sizeof(fds));
    portEXIT_CRITICAL(&s_clients_mux);

    httpd_ws_frame_t ws_pkt;
    memset(&ws_pkt, 0, sizeof(ws_pkt));
    ws_pkt.payload = (uint8_t *) payload.c_str();
    ws_pkt.len = payload.size();
    ws_pkt.type = HTTPD_WS_TYPE_TEXT;

    for (int i = 0; i < WS_SHARES_MAX_CLIENTS; i++) {
        if (fds[i] < 0 || !http_server) {
            continue;
        }
        if (httpd_ws_send_frame_async(http_server, fds[i], &ws_pkt) != ESP_OK) {
            // socket is dead or was reused behind our back
            remove_client(fds[i]);
        }
    }
}

static void ws_shares_task(void *param)
{
    while (true) {
        ws_share_event_t ev;

        if (xQueueReceive(s_queue, &ev, (TickType_t) portMAX_DELAY) != pdPASS) {
            continue;
        }

        // let a burst pile up so it leaves as one frame
        vTaskDelay(pdMS_TO_TICKS(WS_SHARES_COALESCE_MS));

        if (!s_client_count.load(std::memory_order_relaxed)) {
            xQueueReset(s_queue);
            continue;
        }

        PSRAMAllocator allocator;
        JsonDocument doc(&allocator);
        JsonArray arr = doc.to<JsonArray>();

        serialize_event(&ev, arr);
        while (xQueueReceive(s_queue, &ev, (TickType_t) 0) == pdPASS) {
            serialize_event(&ev, arr);
        }

        uint32_t dropped = s_dropped.exchange(0, std::memory_order_relaxed);
        if (dropped) {
            JsonObject o = arr.add<JsonObject>();
            o["t"] = "d";
            o["ts"] = now_ms();
            o["n"] = dropped;
        }

        std::string payload;
        serializeJson(doc, payload);

        send_to_clients(payload);
    }
}

//--------------------------------------------------------------------
// websocket handler
//--------------------------------------------------------------------

esp_err_t ws_shares_handler(httpd_req_t *req)
{
    if (is_network_allowed(req) != ESP_OK) {
        return httpd_resp_send_err(req, HTTPD_401_UNAUTHORIZED, "Unauthorized");
    }

    int fd = httpd_req_to_sockfd(req);

    if (req->method == HTTP_GET) {
        // the handshake already happened, so a full registry means dropping
        // the connection rather than answering with an HTTP status
        if (!add_client(fd)) {
            ESP_LOGW(TAG, "share stream full, refusing fd %d", fd);
            return ESP_FAIL;
        }
        ESP_LOGI(TAG, "share stream client joined (fd %d, %d total)", fd, s_client_count.load(std::memory_order_relaxed));
        return ESP_OK;
    }

    httpd_ws_frame_t ws_pkt;
    memset(&ws_pkt, 0, sizeof(ws_pkt));

    // first pass reads the header only, so we learn the length
    esp_err_t ret = httpd_ws_recv_frame(req, &ws_pkt, 0);
    if (ret != ESP_OK) {
        remove_client(fd);
        return ret;
    }

    if (ws_pkt.type == HTTPD_WS_TYPE_CLOSE) {
        remove_client(fd);
        return ESP_OK;
    }

    // nothing is expected from the client, but the payload still has to be
    // consumed or the next frame's header would be read from the middle of it
    if (ws_pkt.len) {
        if (ws_pkt.len > WS_SHARES_MAX_RX_LEN) {
            ESP_LOGW(TAG, "oversized frame (%d bytes) from fd %d", (int) ws_pkt.len, fd);
            remove_client(fd);
            return ESP_FAIL;
        }

        uint8_t buf[WS_SHARES_MAX_RX_LEN + 1];
        memset(buf, 0, sizeof(buf));
        ws_pkt.payload = buf;

        ret = httpd_ws_recv_frame(req, &ws_pkt, ws_pkt.len);
        if (ret != ESP_OK) {
            remove_client(fd);
            return ret;
        }
    }

    return ESP_OK;
}

void ws_shares_start()
{
    for (int i = 0; i < WS_SHARES_MAX_CLIENTS; i++) {
        s_client_fds[i] = -1;
    }

    s_queue_storage = (uint8_t *) CALLOC(WS_SHARES_QUEUE_LEN, sizeof(ws_share_event_t));
    if (!s_queue_storage) {
        ESP_LOGE(TAG, "no memory for the share queue, stream disabled");
        return;
    }

    s_queue = xQueueCreateStatic(WS_SHARES_QUEUE_LEN, sizeof(ws_share_event_t), s_queue_storage, &s_queue_struct);
    if (!s_queue) {
        ESP_LOGE(TAG, "couldn't create the share queue, stream disabled");
        FREE(s_queue_storage);
        return;
    }

    xTaskCreatePSRAM(&ws_shares_task, "ws_shares", 4096, NULL, 2, NULL);
}
