#pragma once

#include <stdbool.h>
#include <stdint.h>

#include "esp_http_server.h"

// Real time SHA-256 stream, served as a websocket on /api/v2/ws/shares.
//
// Two levels are pushed to the browser:
//   - every nonce the ASIC reports and the ESP32 verifies with a SHA-256d
//     (~0.55/s at 4.8 TH/s with an asic difficulty of 2048)
//   - the pool's verdict for every share that was submitted
//
// Producers run on hot tasks (ASIC_result_task at priority 15, the stratum RX
// loop while holding StratumManager::m_mutex), so they only ever copy a POD
// into a queue with a zero timeout. A low priority task serializes and sends.
// A producer must never touch the socket: httpd_ws_send_frame_async can block
// for up to send_wait_timeout (5 s) when the browser stops draining, which
// would stall mining - the same trap the InfluxDB telemetry lock hit.
//
// The events carry the JSON-RPC id of the submit, and the pool's verdict
// carries the id it answers. Correlating them is left to the browser, which
// already holds the display state; the firmware keeps no pending table.

#define WS_SHARES_MAX_CLIENTS 3

// Pushed for every nonce the ASIC reports.
// hash is the raw SHA-256d in internal (little endian) order.
// submit_id is the JSON-RPC id / SV2 sequence number of the mining.submit, or
// -1 when the nonce did not reach the pool difficulty (or the send failed).
void ws_shares_push_nonce(const uint8_t hash[32], double diff, uint32_t pool_diff, uint32_t asic_diff, uint32_t nbits,
                          const char *jobid, const char *extranonce2, uint32_t ntime, uint32_t nonce,
                          uint32_t version_rolled, int pool, int asic_nr, bool duplicate, int submit_id);

// Pushed when a pool accepts or rejects a share. reason may be NULL.
// submit_id -1 means the pool did not say which share it is answering: SV2
// batches its acknowledgements, so count carries how many of the oldest
// pending shares the verdict covers.
void ws_shares_push_verdict(int pool, int64_t submit_id, bool accepted, const char *reason, uint32_t count = 1);

// Pushed when a pool reconnects: the JSON-RPC id counter restarts at 1, so the
// browser has to drop whatever it still had pending for that pool.
void ws_shares_push_reset(int pool);

esp_err_t ws_shares_handler(httpd_req_t *req);

// Called from the httpd close callback so a client that goes away without a
// CLOSE frame is dropped from the registry.
void ws_shares_on_socket_closed(int sockfd);

void ws_shares_start();
