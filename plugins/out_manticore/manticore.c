/* -*- Mode: C; tab-width: 4; indent-tabs-mode: nil; c-basic-offset: 4 -*- */

/*  Fluent Bit
 *  ==========
 *  Copyright (C) 2015-2026 The Fluent Bit Authors
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

#include <fluent-bit/flb_output_plugin.h>
#include <fluent-bit/flb_output.h>
#include <fluent-bit/flb_http_client.h>
#include <fluent-bit/flb_io.h>
#include <fluent-bit/flb_log_event_decoder.h>
#include <fluent-bit/flb_mem.h>
#include <fluent-bit/flb_pack.h>
#include <fluent-bit/flb_sds.h>
#include <fluent-bit/flb_upstream.h>

#include <msgpack.h>

#include <inttypes.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "manticore.h"

#define MANTICORE_STREAM_OK             0
#define MANTICORE_STREAM_RETRY          1
#define MANTICORE_STREAM_RECORD_ERROR   2

struct manticore_id_list {
    uint64_t *values;
    size_t count;
    size_t capacity;
};

static int append(flb_sds_t *buf, const char *data, size_t len)
{
    return flb_sds_cat_safe(buf, data, len);
}

static int is_query_component_char(unsigned char c)
{
    return (c >= 'a' && c <= 'z') ||
           (c >= 'A' && c <= 'Z') ||
           (c >= '0' && c <= '9') ||
           c == '-' || c == '_' || c == '.' || c == '~';
}

static flb_sds_t build_bulk_uri(const char *table)
{
    int ret;
    size_t i;
    char encoded[4];
    flb_sds_t uri;

    uri = flb_sds_create(FLB_MANTICORE_BULK_URI);
    if (uri == NULL) {
        return NULL;
    }

    for (i = 0; table[i] != '\0'; i++) {
        if (is_query_component_char((unsigned char) table[i])) {
            ret = append(&uri, &table[i], 1);
        }
        else {
            snprintf(encoded, sizeof(encoded), "%%%02X",
                     (unsigned char) table[i]);
            ret = append(&uri, encoded, 3);
        }

        if (ret != 0) {
            flb_sds_destroy(uri);
            return NULL;
        }
    }

    return uri;
}

static char *object_to_json(const msgpack_object *obj, int escape_unicode)
{
    return flb_msgpack_to_json_str(256, obj, escape_unicode);
}

static int key_equals(const msgpack_object *key, const char *name)
{
    size_t len;

    if (key->type != MSGPACK_OBJECT_STR || name == NULL) {
        return FLB_FALSE;
    }

    len = strlen(name);
    if (key->via.str.size != len) {
        return FLB_FALSE;
    }

    return memcmp(key->via.str.ptr, name, len) == 0;
}

static int parse_id(const msgpack_object *value, uint64_t *id)
{
    size_t i;
    uint64_t current;
    unsigned int digit;

    if (value->type == MSGPACK_OBJECT_POSITIVE_INTEGER) {
        if (value->via.u64 == 0) {
            return -1;
        }
        *id = value->via.u64;
        return 0;
    }

    if (value->type != MSGPACK_OBJECT_STR || value->via.str.size == 0) {
        return -1;
    }

    current = 0;
    for (i = 0; i < value->via.str.size; i++) {
        if (value->via.str.ptr[i] < '0' || value->via.str.ptr[i] > '9') {
            return -1;
        }
        digit = value->via.str.ptr[i] - '0';
        if (current > (UINT64_MAX - digit) / 10) {
            return -1;
        }
        current = current * 10 + digit;
    }

    if (current == 0) {
        return -1;
    }
    *id = current;
    return 0;
}

static int append_id(struct manticore_id_list *ids, uint64_t id)
{
    size_t capacity;
    uint64_t *values;

    if (ids->count == ids->capacity) {
        capacity = ids->capacity == 0 ? 64 : ids->capacity * 2;
        if (capacity < ids->capacity ||
            capacity > SIZE_MAX / sizeof(uint64_t)) {
            return -1;
        }
        values = flb_realloc(ids->values, capacity * sizeof(uint64_t));
        if (values == NULL) {
            return -1;
        }
        ids->values = values;
        ids->capacity = capacity;
    }

    ids->values[ids->count++] = id;
    return 0;
}

static int compare_ids(const void *left, const void *right)
{
    uint64_t a;
    uint64_t b;

    a = *(const uint64_t *) left;
    b = *(const uint64_t *) right;
    return (a > b) - (a < b);
}

static int validate_record(struct flb_out_manticore *ctx,
                           const msgpack_object *body, uint64_t *id)
{
    int i;
    int id_found;
    const msgpack_object_kv *entry;

    if (body == NULL || body->type != MSGPACK_OBJECT_MAP) {
        flb_plg_error(ctx->ins, "log record body must be a map");
        return -1;
    }

    entry = body->via.map.ptr;
    id_found = FLB_FALSE;
    for (i = 0; i < body->via.map.size; i++) {
        if (entry[i].key.type != MSGPACK_OBJECT_STR) {
            flb_plg_error(ctx->ins, "record keys must be strings");
            return -1;
        }

        if (!key_equals(&entry[i].key, ctx->id_key)) {
            continue;
        }

        if (id_found || parse_id(&entry[i].val, id) != 0) {
            flb_plg_error(ctx->ins,
                          "record key '%s' must be a unique, non-zero numeric ID",
                          ctx->id_key);
            return -1;
        }
        id_found = FLB_TRUE;
    }

    if (!id_found) {
        flb_plg_error(ctx->ins,
                      "record key '%s' must contain a non-zero numeric ID",
                      ctx->id_key);
        return -1;
    }

    return 0;
}

static int validate_events(struct flb_out_manticore *ctx,
                           const void *data, size_t bytes)
{
    int ret;
    size_t i;
    uint64_t id;
    struct flb_log_event event;
    struct flb_log_event_decoder decoder;
    struct manticore_id_list ids = {0};

    ret = flb_log_event_decoder_init(&decoder, (char *) data, bytes);
    if (ret != FLB_EVENT_DECODER_SUCCESS) {
        flb_plg_error(ctx->ins, "could not initialize log event decoder: %s",
                      flb_log_event_decoder_get_error_description(ret));
        return MANTICORE_STREAM_RETRY;
    }

    while (flb_log_event_decoder_next(&decoder, &event) ==
           FLB_EVENT_DECODER_SUCCESS) {
        if (validate_record(ctx, event.body, &id) != 0) {
            flb_free(ids.values);
            flb_log_event_decoder_destroy(&decoder);
            return MANTICORE_STREAM_RECORD_ERROR;
        }
        if (append_id(&ids, id) != 0) {
            flb_plg_error(ctx->ins, "could not allocate document ID preflight");
            flb_free(ids.values);
            flb_log_event_decoder_destroy(&decoder);
            return MANTICORE_STREAM_RETRY;
        }
    }

    ret = flb_log_event_decoder_get_last_result(&decoder);
    if (ret != FLB_EVENT_DECODER_SUCCESS) {
        flb_plg_error(ctx->ins, "could not decode log event: %s",
                      flb_log_event_decoder_get_error_description(ret));
    }
    else if (ids.count == 0) {
        flb_plg_error(ctx->ins, "bulk_import requires at least one record");
        ret = MANTICORE_STREAM_RECORD_ERROR;
    }
    else if (ids.count > 1) {
        qsort(ids.values, ids.count, sizeof(uint64_t), compare_ids);
        for (i = 1; i < ids.count; i++) {
            if (ids.values[i - 1] == ids.values[i]) {
                flb_plg_error(ctx->ins,
                              "record key '%s' must be unique within a chunk",
                              ctx->id_key);
                ret = MANTICORE_STREAM_RECORD_ERROR;
                break;
            }
        }
    }

    flb_free(ids.values);
    flb_log_event_decoder_destroy(&decoder);
    return ret == FLB_EVENT_DECODER_SUCCESS ?
           MANTICORE_STREAM_OK : MANTICORE_STREAM_RECORD_ERROR;
}

static flb_sds_t format_record(struct flb_out_manticore *ctx,
                               const msgpack_object *body)
{
    int ret;
    int i;
    int id_len;
    int fields;
    uint64_t numeric_id;
    char id_json[32];
    char *key_json;
    char *value_json;
    flb_sds_t out;
    const msgpack_object *id;
    const msgpack_object_kv *entry;

    if (body == NULL || body->type != MSGPACK_OBJECT_MAP) {
        return NULL;
    }

    id = NULL;
    fields = 0;
    entry = body->via.map.ptr;

    for (i = 0; i < body->via.map.size; i++) {
        if (key_equals(&entry[i].key, ctx->id_key)) {
            id = &entry[i].val;
        }
        else {
            fields++;
        }
    }

    if (id == NULL || parse_id(id, &numeric_id) != 0) {
        return NULL;
    }
    id_len = snprintf(id_json, sizeof(id_json), "%" PRIu64, numeric_id);
    if (id_len <= 0 || id_len >= sizeof(id_json)) {
        return NULL;
    }

    out = flb_sds_create_size(512);
    if (out == NULL) {
        return NULL;
    }

    ret = append(&out, "{\"", sizeof("{\"") - 1);
    ret |= append(&out, ctx->bulk_action, strlen(ctx->bulk_action));
    ret |= append(&out, "\":{\"table\":", sizeof("\":{\"table\":") - 1);
    ret |= append(&out, ctx->table_json, flb_sds_len(ctx->table_json));
    ret |= append(&out, ",\"id\":", sizeof(",\"id\":") - 1);
    ret |= append(&out, id_json, id_len);
    ret |= append(&out, ",\"doc\":{", sizeof(",\"doc\":{") - 1);

    fields = 0;
    for (i = 0; i < body->via.map.size; i++) {
        if (key_equals(&entry[i].key, ctx->id_key)) {
            continue;
        }

        key_json = object_to_json(&entry[i].key,
                                  ctx->config->json_escape_unicode);
        value_json = object_to_json(&entry[i].val,
                                    ctx->config->json_escape_unicode);
        if (key_json == NULL || value_json == NULL) {
            flb_free(key_json);
            flb_free(value_json);
            flb_sds_destroy(out);
            return NULL;
        }

        if (fields++ > 0) {
            ret |= append(&out, ",", sizeof(",") - 1);
        }
        ret |= append(&out, key_json, strlen(key_json));
        ret |= append(&out, ":", sizeof(":") - 1);
        ret |= append(&out, value_json, strlen(value_json));
        flb_free(key_json);
        flb_free(value_json);

        if (ret != 0) {
            flb_sds_destroy(out);
            return NULL;
        }
    }

    ret |= append(&out, "}}}\n", sizeof("}}}\n") - 1);
    if (ret != 0) {
        flb_sds_destroy(out);
        return NULL;
    }

    return out;
}

static int write_all(struct flb_connection *connection,
                     const void *data, size_t length)
{
    int ret;
    size_t written;

    written = 0;
    ret = flb_io_net_write(connection, data, length, &written);
    if (ret == -1 || written != length) {
        return -1;
    }

    return 0;
}

static int write_chunk(struct flb_connection *connection,
                       const void *data, size_t length)
{
    int len;
    char header[32];

    len = snprintf(header, sizeof(header), "%zx\r\n", length);
    if (len <= 0 || len >= sizeof(header)) {
        return -1;
    }

    if (write_all(connection, header, len) != 0 ||
        write_all(connection, data, length) != 0 ||
        write_all(connection, "\r\n", 2) != 0) {
        return -1;
    }

    return 0;
}

static void inspect_item_status(const msgpack_object *item,
                                int *has_status, int *retryable)
{
    int i;
    msgpack_object action;
    msgpack_object key;
    msgpack_object value;

    if (item->type != MSGPACK_OBJECT_MAP || item->via.map.size != 1) {
        return;
    }

    action = item->via.map.ptr[0].val;
    if (action.type != MSGPACK_OBJECT_MAP) {
        return;
    }

    for (i = 0; i < action.via.map.size; i++) {
        key = action.via.map.ptr[i].key;
        value = action.via.map.ptr[i].val;
        if (key.type != MSGPACK_OBJECT_STR || key.via.str.size != 6 ||
            memcmp(key.via.str.ptr, "status", 6) != 0) {
            continue;
        }

        if (value.type == MSGPACK_OBJECT_POSITIVE_INTEGER) {
            *has_status = FLB_TRUE;
            if (value.via.u64 == 408 || value.via.u64 == 429 ||
                value.via.u64 >= 500) {
                *retryable = FLB_TRUE;
            }
            return;
        }
    }
}

static void inspect_response_items(const msgpack_object *items,
                                   int *has_status, int *retryable)
{
    int i;

    if (items->type != MSGPACK_OBJECT_ARRAY) {
        return;
    }

    for (i = 0; i < items->via.array.size; i++) {
        inspect_item_status(&items->via.array.ptr[i], has_status, retryable);
    }
}

static int response_ok(struct flb_out_manticore *ctx,
                       struct flb_http_client *client)
{
    int i;
    int ret;
    int root_type;
    int errors;
    int has_status;
    int retryable;
    char *packed;
    size_t packed_size;
    size_t offset;
    msgpack_object root;
    msgpack_object key;
    msgpack_object value;
    msgpack_unpacked result;

    packed = NULL;
    packed_size = 0;
    errors = -1;
    has_status = FLB_FALSE;
    retryable = FLB_FALSE;
    if (client->resp.payload_size > 0) {
        ret = flb_pack_json(client->resp.payload, client->resp.payload_size,
                            &packed, &packed_size, &root_type, NULL);
        if (ret == 0) {
            msgpack_unpacked_init(&result);
            offset = 0;
            ret = msgpack_unpack_next(&result, packed, packed_size, &offset);
            if (ret == MSGPACK_UNPACK_SUCCESS) {
                root = result.data;
                if (root.type == MSGPACK_OBJECT_MAP) {
                    for (i = 0; i < root.via.map.size; i++) {
                        key = root.via.map.ptr[i].key;
                        value = root.via.map.ptr[i].val;
                        if (key.type == MSGPACK_OBJECT_STR &&
                            key.via.str.size == 6 &&
                            memcmp(key.via.str.ptr, "errors", 6) == 0 &&
                            value.type == MSGPACK_OBJECT_BOOLEAN) {
                            errors = value.via.boolean;
                        }
                        else if (key.type == MSGPACK_OBJECT_STR &&
                                 key.via.str.size == 5 &&
                                 memcmp(key.via.str.ptr, "items", 5) == 0) {
                            inspect_response_items(&value, &has_status, &retryable);
                        }
                    }
                }
            }
            msgpack_unpacked_destroy(&result);
        }
        flb_free(packed);
    }

    if (client->resp.status >= 200 && client->resp.status < 300) {
        if (errors == FLB_FALSE) {
            return FLB_OK;
        }

        if (errors == FLB_TRUE && retryable == FLB_TRUE) {
            flb_plg_warn(ctx->ins,
                         "Manticore /bulk returned a retryable item error");
            return FLB_RETRY;
        }

        flb_plg_error(ctx->ins, "invalid or failed Manticore /bulk response: %.*s",
                      (int) client->resp.payload_size,
                      client->resp.payload);
        return FLB_ERROR;
    }

    if (client->resp.payload_size > 0) {
        flb_plg_error(ctx->ins, "Manticore /bulk returned HTTP %d: %.*s",
                      client->resp.status,
                      (int) client->resp.payload_size,
                      client->resp.payload);
    }
    else {
        flb_plg_error(ctx->ins, "Manticore /bulk returned HTTP %d",
                      client->resp.status);
    }

    if (retryable == FLB_TRUE || client->resp.status == 408 ||
        client->resp.status == 429) {
        return FLB_RETRY;
    }

    if (client->resp.status >= 500 && has_status == FLB_FALSE) {
        return FLB_RETRY;
    }

    return FLB_ERROR;
}

static int stream_events(struct flb_out_manticore *ctx,
                         struct flb_connection *connection,
                         const void *data, size_t bytes)
{
    int ret;
    flb_sds_t line;
    flb_sds_t chunk;
    struct flb_log_event event;
    struct flb_log_event_decoder decoder;

    ret = flb_log_event_decoder_init(&decoder, (char *) data, bytes);
    if (ret != FLB_EVENT_DECODER_SUCCESS) {
        return MANTICORE_STREAM_RETRY;
    }

    chunk = flb_sds_create_size(ctx->stream_chunk_size);
    if (chunk == NULL) {
        flb_log_event_decoder_destroy(&decoder);
        return MANTICORE_STREAM_RETRY;
    }

    while ((ret = flb_log_event_decoder_next(&decoder, &event)) ==
           FLB_EVENT_DECODER_SUCCESS) {
        line = format_record(ctx, event.body);
        if (line == NULL) {
            ret = MANTICORE_STREAM_RETRY;
            break;
        }

        if (flb_sds_len(chunk) > 0 &&
            flb_sds_len(chunk) + flb_sds_len(line) > ctx->stream_chunk_size) {
            if (write_chunk(connection, chunk, flb_sds_len(chunk)) != 0) {
                flb_sds_destroy(line);
                ret = MANTICORE_STREAM_RETRY;
                break;
            }
            flb_sds_len_set(chunk, 0);
            chunk[0] = '\0';
        }

        if (flb_sds_len(line) > ctx->stream_chunk_size) {
            ret = write_chunk(connection, line, flb_sds_len(line));
        }
        else {
            ret = append(&chunk, line, flb_sds_len(line));
        }
        flb_sds_destroy(line);

        if (ret != 0) {
            ret = MANTICORE_STREAM_RETRY;
            break;
        }
    }

    if (ret != MANTICORE_STREAM_RETRY &&
        ret != MANTICORE_STREAM_RECORD_ERROR) {
        ret = flb_log_event_decoder_get_last_result(&decoder);
        if (ret == FLB_EVENT_DECODER_SUCCESS) {
            ret = MANTICORE_STREAM_OK;
        }
        else {
            flb_plg_error(ctx->ins, "could not decode log event: %s",
                          flb_log_event_decoder_get_error_description(ret));
            ret = MANTICORE_STREAM_RECORD_ERROR;
        }
    }

    if (ret == MANTICORE_STREAM_OK && flb_sds_len(chunk) > 0) {
        if (write_chunk(connection, chunk, flb_sds_len(chunk)) != 0) {
            ret = MANTICORE_STREAM_RETRY;
        }
    }

    flb_sds_destroy(chunk);
    flb_log_event_decoder_destroy(&decoder);
    return ret;
}

static int send_stream(struct flb_out_manticore *ctx,
                       const void *data, size_t bytes)
{
    int ret;
    int result;
    size_t sent;
    struct flb_connection *connection;
    struct flb_http_client *client;

    /* A permanent record error must not follow already transmitted records. */
    ret = validate_events(ctx, data, bytes);
    if (ret != MANTICORE_STREAM_OK) {
        return ret == MANTICORE_STREAM_RETRY ? FLB_RETRY : FLB_ERROR;
    }

    connection = flb_upstream_conn_get(ctx->u);
    if (connection == NULL) {
        return FLB_RETRY;
    }

    client = flb_http_client(connection, FLB_HTTP_POST,
                             ctx->bulk_uri,
                             NULL, 0, NULL, 0, NULL, 0);
    if (client == NULL) {
        flb_upstream_conn_release(connection);
        return FLB_RETRY;
    }

    flb_http_remove_header(client, "Content-Length", 14);
    flb_http_remove_header(client, "Connection", 10);
    client->body_len = -1;
    flb_http_add_header(client, "Content-Type", 12,
                        "application/x-ndjson", 20);
    flb_http_add_header(client, "Transfer-Encoding", 17, "chunked", 7);
    flb_http_add_header(client, "Connection", 10, "close", 5);
    flb_http_add_header(client, "User-Agent", 10,
                        "Fluent-Bit-Manticore", 20);
    flb_http_buffer_size(client, ctx->buffer_size);

    if (ctx->http_user != NULL) {
        flb_http_basic_auth(client, ctx->http_user, ctx->http_passwd);
    }

    sent = 0;
    ret = flb_http_do_request(client, &sent);
    if (ret != FLB_HTTP_MORE) {
        result = FLB_RETRY;
        goto done;
    }

    ret = stream_events(ctx, connection, data, bytes);
    if (ret != MANTICORE_STREAM_OK) {
        result = ret == MANTICORE_STREAM_RECORD_ERROR ? FLB_ERROR : FLB_RETRY;
        goto done;
    }

    if (write_all(connection, "0\r\n\r\n", 5) != 0) {
        result = FLB_RETRY;
        goto done;
    }

    do {
        ret = flb_http_get_response_data(client, 0);
    } while (ret == FLB_HTTP_MORE || ret == FLB_HTTP_CHUNK_AVAILABLE);

    if (ret != FLB_HTTP_OK) {
        result = FLB_RETRY;
        goto done;
    }

    result = response_ok(ctx, client);

done:
    /* Closing the session releases Manticore's bulk_import reservation. */
    flb_upstream_conn_recycle(connection, FLB_FALSE);
    flb_http_client_destroy(client);
    flb_upstream_conn_release(connection);
    return result;
}

static int cb_manticore_init(struct flb_output_instance *ins,
                             struct flb_config *config, void *data)
{
    int io_flags;
    int ret;
    char *table_json;
    msgpack_object table;
    struct flb_out_manticore *ctx;

    (void) data;

    ctx = flb_calloc(1, sizeof(struct flb_out_manticore));
    if (ctx == NULL) {
        return -1;
    }

    ctx->ins = ins;
    ctx->config = config;
    flb_output_net_default("127.0.0.1", FLB_MANTICORE_DEFAULT_PORT, ins);

    ret = flb_output_config_map_set(ins, ctx);
    if (ret == -1 || ctx->table == NULL || ctx->table[0] == '\0') {
        flb_plg_error(ins, "table is required");
        flb_free(ctx);
        return -1;
    }

    if (strcasecmp(ctx->action, "insert") != 0 &&
        strcasecmp(ctx->action, "create") != 0) {
        flb_plg_error(ins, "action must be 'insert' or 'create'");
        flb_free(ctx);
        return -1;
    }

    ctx->bulk_action = strcasecmp(ctx->action, "create") == 0 ?
                       "create" : "insert";

    if (ctx->stream_chunk_size == 0) {
        flb_plg_error(ins, "stream_chunk_size must be greater than zero");
        flb_free(ctx);
        return -1;
    }

    table.type = MSGPACK_OBJECT_STR;
    table.via.str.ptr = ctx->table;
    table.via.str.size = strlen(ctx->table);
    table_json = object_to_json(&table, config->json_escape_unicode);
    if (table_json == NULL) {
        flb_free(ctx);
        return -1;
    }
    ctx->table_json = flb_sds_create(table_json);
    flb_free(table_json);
    if (ctx->table_json == NULL) {
        flb_free(ctx);
        return -1;
    }

    ctx->bulk_uri = build_bulk_uri(ctx->table);
    if (ctx->bulk_uri == NULL) {
        flb_sds_destroy(ctx->table_json);
        flb_free(ctx);
        return -1;
    }

    /* A persistent session would keep the bulk_import reservation active. */
    ins->net_setup.keepalive = FLB_FALSE;

    io_flags = ins->use_tls == FLB_TRUE ? FLB_IO_TLS : FLB_IO_TCP;
    if (ins->host.ipv6 == FLB_TRUE) {
        io_flags |= FLB_IO_IPV6;
    }

    ctx->u = flb_upstream_create(config, ins->host.name, ins->host.port,
                                 io_flags, ins->tls);
    if (ctx->u == NULL) {
        flb_sds_destroy(ctx->bulk_uri);
        flb_sds_destroy(ctx->table_json);
        flb_free(ctx);
        return -1;
    }
    flb_output_upstream_set(ctx->u, ins);
    flb_output_set_context(ins, ctx);
    flb_output_set_http_debug_callbacks(ins);
    return 0;
}

static void cb_manticore_flush(struct flb_event_chunk *event_chunk,
                               struct flb_output_flush *out_flush,
                               struct flb_input_instance *ins,
                               void *out_context,
                               struct flb_config *config)
{
    int ret;
    struct flb_out_manticore *ctx;

    (void) ins;
    (void) config;

    ctx = out_context;
    ret = send_stream(ctx, event_chunk->data, event_chunk->size);
    FLB_OUTPUT_RETURN(ret);
}

static int cb_manticore_exit(void *data, struct flb_config *config)
{
    struct flb_out_manticore *ctx;

    (void) config;
    ctx = data;
    if (ctx == NULL) {
        return 0;
    }

    if (ctx->u != NULL) {
        flb_upstream_destroy(ctx->u);
    }
    if (ctx->table_json != NULL) {
        flb_sds_destroy(ctx->table_json);
    }
    if (ctx->bulk_uri != NULL) {
        flb_sds_destroy(ctx->bulk_uri);
    }
    flb_free(ctx);
    return 0;
}

static struct flb_config_map config_map[] = {
    {
     FLB_CONFIG_MAP_STR, "table", NULL,
     0, FLB_TRUE, offsetof(struct flb_out_manticore, table),
     "Target Manticore table (must already exist)"
    },
    {
     FLB_CONFIG_MAP_STR, "action", "insert",
     0, FLB_TRUE, offsetof(struct flb_out_manticore, action),
     "Manticore bulk import action: insert or create"
    },
    {
     FLB_CONFIG_MAP_STR, "id_key", "id",
     0, FLB_TRUE, offsetof(struct flb_out_manticore, id_key),
     "Required top-level non-zero numeric document ID, removed from doc"
    },
    {
     FLB_CONFIG_MAP_SIZE, "stream_chunk_size", "64K",
     0, FLB_TRUE, offsetof(struct flb_out_manticore, stream_chunk_size),
     "Maximum uncompressed NDJSON bytes buffered per HTTP chunk"
    },
    {
     FLB_CONFIG_MAP_SIZE, "buffer_size", "64K",
     0, FLB_TRUE, offsetof(struct flb_out_manticore, buffer_size),
     "Maximum response buffer size"
    },
    {
     FLB_CONFIG_MAP_STR, "http_user", NULL,
     0, FLB_TRUE, offsetof(struct flb_out_manticore, http_user),
     "HTTP Basic authentication user"
    },
    {
     FLB_CONFIG_MAP_STR, "http_passwd", "",
     0, FLB_TRUE, offsetof(struct flb_out_manticore, http_passwd),
     "HTTP Basic authentication password"
    },
    {0}
};

struct flb_output_plugin out_manticore_plugin = {
    .name        = "manticore",
    .description = "Manticore Search native streaming output",
    .cb_init     = cb_manticore_init,
    .cb_pre_run  = NULL,
    .cb_flush    = cb_manticore_flush,
    .cb_exit     = cb_manticore_exit,
    .workers     = 2,
    .config_map  = config_map,
    .event_type  = FLB_OUTPUT_LOGS,
    .flags       = FLB_OUTPUT_NET | FLB_IO_OPT_TLS
};
