/*
Tencent is pleased to support the open source community by making PhxQueue available.
Copyright (C) 2017 THL A29 Limited, a Tencent company. All rights reserved.
Licensed under the BSD 3-Clause License (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at

<https://opensource.org/licenses/BSD-3-Clause>

Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.
*/



#include "co_routine.h"

#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <errno.h>
#include <set>
#include <stack>
#include <sys/time.h>
#include <unistd.h>
#include <vector>

#include "mqttbroker_server_config.h"
#include "server_mgr.h"
#include "publish/publish_memory.h"
#include "publish/publish_mgr.h"


using namespace std;


struct Task {
    stCoRoutine_t *co;
};

struct DispatchCtx {
    stCoRoutine_t *co{nullptr};
    uint64_t session_id{0uL};
    const MqttBrokerServerConfig *config{nullptr};
};

static void *poll_routine(void *arg) {
    co_enable_hook_sys();

    DispatchCtx *ctx{static_cast<DispatchCtx *>(arg)};

    while (true) {
        printf("co %p poll start config %p sleep %d\n",
               co_self(), ctx->config, ctx->config->publish_sleep_time_ms());
        poll(nullptr, 0, 5000);
        printf("co %p poll end config %p sleep %d\n",
               co_self(), ctx->config, ctx->config->publish_sleep_time_ms());
    }

    return 0;
}

void ShowUsage(const char *program) {
    printf("\n");
    printf("Usage: %s [-c <config>] [-v]\n", program);
    printf("\n");

    exit(0);
}

int Loop(void *) {
    stCoRoutine_t *co = 0;
    vector<Task> v;
    co_create(&co, nullptr, poll_routine, &v);
    printf("create\n");
    co_resume(co);

    return 0;
}


void *RoutineRun(void *arg) {
    co_enable_hook_sys();
    printf("%s:%d co %p begin\n", __func__, __LINE__, co_self());

    DispatchCtx *ctx{static_cast<DispatchCtx *>(arg)};
    // test
    while (true) {
        printf("%s:%d co %p begin ctx %p session_id %" PRIu64 "\n", __func__, __LINE__,
               co_self(), ctx, ctx->session_id);
        poll(nullptr, 0, 5000);
        printf("%s:%d co %p end ctx %p session_id %" PRIu64 "\n", __func__, __LINE__,
               co_self(), ctx, ctx->session_id);
    }

    delete ctx;
    ctx = nullptr;
}

int main(int argc,char **argv) {
    const char *config_file{nullptr};
    int c;
    while (EOF != (c = getopt(argc, argv, "c:v"))) {
        switch (c) {
            case 'c': config_file = optarg; break;

            case 'v':
            default: ShowUsage(argv[0]); break;
        }
    }

    if (nullptr == config_file) ShowUsage(argv[0]);

    MqttBrokerServerConfig config;

    if (!config.Read(config_file)) ShowUsage(argv[0]);

    phxqueue_phxrpc::mqttbroker::ServerMgr server_mgr(&(config.GetHshaServerConfig()));

    phxqueue_phxrpc::mqttbroker::PublishQueue::SetInstance(
            new phxqueue_phxrpc::mqttbroker::PublishQueue(config.max_publish_queue_size()));

    phxqueue_phxrpc::mqttbroker::PublishMgr::SetInstance(
            new phxqueue_phxrpc::mqttbroker::PublishMgr(&config));

    phxqueue_phxrpc::mqttbroker::PublishMgr::GetInstance()->CreateSession(1234);
    phxqueue_phxrpc::mqttbroker::PublishMgr::GetInstance()->CreateSession(5678);

    //{
    //stCoRoutineAttr_t attr;
    //attr.stack_size = 1024 * 128;

    //DispatchCtx *ctx{new DispatchCtx};
    //ctx->co = nullptr;
    //ctx->session_id = 1234;
    //ctx->config = nullptr;
    //co_create(&(ctx->co), &attr, RoutineRun, ctx);
    //co_resume(ctx->co);
    //}

    //{
    //stCoRoutineAttr_t attr;
    //attr.stack_size = 1024 * 128;

    //DispatchCtx *ctx{new DispatchCtx};
    //ctx->co = nullptr;
    //ctx->session_id = 5678;
    //ctx->config = nullptr;
    //co_create(&(ctx->co), &attr, RoutineRun, ctx);
    //co_resume(ctx->co);
    //}

    //co_eventloop(co_get_epoll_ct(), nullptr, nullptr);

    // run forever
    while (true) {
        sleep(1);
    }

    //vector<Task> v;

    //printf("--------------------- main -------------------\n");
    //vector<Task> v2 = v;
    ////poll_routine(&v2);
    //printf("--------------------- routine -------------------\n");

    //for (int i = 0; i < 2; i++) {
    //    DispatchCtx ctx;
    //    ctx.co = nullptr;
    //    ctx.config = &config;
    //    co_create(&(ctx.co), nullptr, poll_routine, &ctx);
    //    printf("routine i %d\n", i);
    //    co_resume(ctx.co);
    //}

    ////co_eventloop(co_get_epoll_ct(), Loop, 0);
    //co_eventloop(co_get_epoll_ct(), nullptr, 0);

    return 0;
}

//./example_poll 127.0.0.1 12365 127.0.0.1 12222 192.168.1.1 1000 192.168.1.2 1111

