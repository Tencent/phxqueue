/*
Tencent is pleased to support the open source community by making PhxQueue available.
Copyright (C) 2017 THL A29 Limited, a Tencent company. All rights reserved.
Licensed under the BSD 3-Clause License (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at

<https://opensource.org/licenses/BSD-3-Clause>

Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.
*/


#include <cinttypes>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <errno.h>
#include <fcntl.h>
#include <iostream>
#include <memory>
#include <netinet/in.h>
#include <poll.h>
#include <sys/socket.h>
#include <time.h>
#include <unistd.h>


#include "phxqueue/comm.h"
#include "phxqueue/plugin.h"
#include "phxqueue/store.h"
#include "phxqueue_phxrpc/comm.h"
#include "phxqueue_phxrpc/config.h"
#include "phxqueue_phxrpc/plugin.h"

#include "phxqueue_phxrpc/app/store/store_client.h"


using namespace phxqueue;
using namespace phxqueue_phxrpc;
using namespace std;

void ShowUsage(const char *proc) {
    printf("\t%s -f <global_config_path> -t <topic_id> -s <store_id> -q <queue_id> -c <consumer_group_id> [-r (random get)]\n", proc);
}

phxqueue::comm::RetCode Get(const phxqueue::comm::proto::GetRequest &req,
                            phxqueue::comm::proto::GetResponse &resp) {

    static __thread StoreClient store_client;
    auto ret = store_client.ProtoGet(req, resp);
    if (phxqueue::comm::RetCode::RET_OK != ret) {
        NLErr("ProtoGet ret %d", phxqueue::comm::as_integer(ret));
    }
    return ret;
}

void PrintItems(const phxqueue::comm::proto::GetResponse &resp) {
    NLInfo("items_size %d", (int)resp.items_size());
    for (size_t i{0}; i < resp.items_size(); ++i) {
        auto &&item = resp.items(i);
        NLInfo("cursor_id %" PRIu64 " buf %s", (uint64_t)item.cursor_id(), item.buffer().c_str());
    }
}


phxqueue::comm::RetCode OneCall(phxqueue::comm::proto::GetRequest &req, phxqueue::comm::proto::GetResponse &resp) {
    phxqueue::store::StoreMasterClient<phxqueue::comm::proto::GetRequest, phxqueue::comm::proto::GetResponse> store_master_client;
    phxqueue::comm::RetCode ret = store_master_client.ClientCall(req, resp, bind(&Get, placeholders::_1, placeholders::_2));



    return ret;
}


int main(int argc, char ** argv) {
    const char *global_config_path = nullptr;
    int topic_id = -1;
    int store_id = -1;
    int queue_id = -1;
    int consumer_group_id = -1;
    bool random = false;

    for (int i = 0; i < argc; ++i) {
		if (strcmp(argv[i], "--help") == 0) {
			ShowUsage(argv[0]);
			return 0;
		} else if (strcmp(argv[i], "-f") == 0) {
            global_config_path = argv[++i];
            continue;
        } else if (strcmp(argv[i], "-t") == 0) {
            topic_id = std::atoi(argv[++i]);
            continue;
        } else if (strcmp(argv[i], "-s") == 0) {
            store_id = std::atoi(argv[++i]);
            continue;
        }  else if (strcmp(argv[i], "-q") == 0) {
            queue_id = std::atoi(argv[++i]);
            continue;
        }  else if (strcmp(argv[i], "-c") == 0) {
            consumer_group_id = std::atoi(argv[++i]);
            continue;
        } else if (strcmp(argv[i], "-r") == 0) {
            random = true;
            continue;
        }
    }

    if (nullptr == global_config_path || -1 == topic_id || -1 == store_id || -1 == queue_id || -1 == consumer_group_id) {
        ShowUsage(argv[0]);
        return 0;
    }


    phxqueue::comm::Logger::GetInstance()->SetLogFunc([](const int log_level, const char *format, va_list args)->void {
            if (log_level <= static_cast<int>(phxqueue::comm::LogLevel::Error)) return;
            vprintf(format, args);
            printf("\n");
        });


    phxqueue::comm::proto::GetRequest req;
    phxqueue::comm::proto::GetResponse resp;

    req.set_topic_id(topic_id);
    req.set_store_id(store_id);
    req.set_queue_id(queue_id);
    req.set_limit(100);
    req.set_consumer_group_id(consumer_group_id);
    req.set_prev_cursor_id(-1);
    req.set_next_cursor_id(-1);
    req.set_random(random);

    while (1) {
        auto ret = OneCall(req, resp);
        NLInfo("Dequeue ret %d", phxqueue::comm::as_integer(ret));

        req.set_prev_cursor_id(resp.next_cursor_id());
        req.set_next_cursor_id(resp.next_cursor_id());


        if (phxqueue::comm::as_integer(ret) < 0) break;
        PrintItems(resp);

        sleep(1);
        //if (0 == resp.items_size()) break;
    }


    return 0;
}

