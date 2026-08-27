/* -*- Mode: C; tab-width: 4; indent-tabs-mode: nil; c-basic-offset: 4 -*- */

/*  Fluent Bit
 *  ==========
 *  Copyright (C) 2026 The Fluent Bit Authors
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

#include <fluent-bit.h>
#include <fluent-bit/flb_time.h>

#include "flb_tests_runtime.h"

static pthread_mutex_t result_mutex = PTHREAD_MUTEX_INITIALIZER;
static int output_count;

static int cb_count_output(void *record, size_t size, void *data)
{
    (void) size;
    (void) data;

    pthread_mutex_lock(&result_mutex);
    output_count++;
    pthread_mutex_unlock(&result_mutex);

    flb_free(record);

    return 0;
}

static int get_output_count(void)
{
    int count;

    pthread_mutex_lock(&result_mutex);
    count = output_count;
    pthread_mutex_unlock(&result_mutex);

    return count;
}

static void test_diskstats(void)
{
    int ret;
    int input_fd;
    int output_fd;
    int count;
    uint64_t elapsed_ms;
    flb_ctx_t *ctx;
    struct flb_time start;
    struct flb_time end;
    struct flb_time diff;
    struct flb_lib_out_cb callback;

    output_count = 0;
    elapsed_ms = 0;
    callback.cb = cb_count_output;
    callback.data = NULL;

    ctx = flb_create();
    TEST_CHECK(ctx != NULL);

    ret = flb_service_set(ctx,
                          "Flush", "0.2",
                          "Grace", "1",
                          "Log_Level", "error",
                          NULL);
    TEST_CHECK(ret == 0);

    input_fd = flb_input(ctx, (char *) "node_exporter_metrics", NULL);
    TEST_CHECK(input_fd >= 0);

    ret = flb_input_set(ctx, input_fd,
                        "metrics", "diskstats",
                        "scrape_interval", "1",
                        NULL);
    TEST_CHECK(ret == 0);

    output_fd = flb_output(ctx, (char *) "lib", &callback);
    TEST_CHECK(output_fd >= 0);

    ret = flb_output_set(ctx, output_fd,
                         "format", "json",
                         NULL);
    TEST_CHECK(ret == 0);

    ret = flb_start(ctx);
    TEST_CHECK(ret == 0);

    flb_time_get(&start);
    count = get_output_count();
    while (count == 0 && elapsed_ms < 5000) {
        flb_time_msleep(100);
        count = get_output_count();
        flb_time_get(&end);
        flb_time_diff(&end, &start, &diff);
        elapsed_ms = flb_time_to_nanosec(&diff) / 1000000;
    }

    TEST_CHECK(count > 0);

    flb_stop(ctx);
    flb_destroy(ctx);
}

TEST_LIST = {
    {"diskstats", test_diskstats},
    {NULL, NULL}
};
