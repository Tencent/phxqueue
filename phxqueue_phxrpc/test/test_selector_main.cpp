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
#include "phxqueue/producer.h"
#include "phxqueue_phxrpc/comm.h"
#include "phxqueue_phxrpc/config.h"
#include "phxqueue_phxrpc/plugin.h"


using namespace phxqueue;
using namespace std;

void ShowUsage(const char *proc) {
    printf("\t%s -f <global_config_path> -t <topic_id> -p <pub_id> -u <uin> [-r](retry_switch_store)\n", proc);
}

int main(int argc, char ** argv) {
    const char *global_config_path = nullptr;
    int topic_id = -1;
    int pub_id = -1;
    uint64_t uin = -1;
    bool retry_switch_store = 0;

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
        } else if (strcmp(argv[i], "-p") == 0) {
            pub_id = std::atoi(argv[++i]);
            continue;
        }  else if (strcmp(argv[i], "-u") == 0) {
            uin = std::atoi(argv[++i]);
            continue;
        } else if (strcmp(argv[i], "-r") == 0) {
            retry_switch_store = true;
            continue;
        }
    }

    if (nullptr == global_config_path || -1 == topic_id || -1 == pub_id) {
        ShowUsage(argv[0]);
        return 0;
    }


    phxqueue::comm::Logger::GetInstance()->SetLogFunc([](const int log_level, const char *format, va_list args)->void {
            if (log_level > static_cast<int>(phxqueue::comm::LogLevel::Error)) return;
            vprintf(format, args);
            printf("\n");
        });

    string phxqueue_global_config_path = global_config_path;
    phxqueue::plugin::ConfigFactory::SetConfigFactoryCreateFunc([phxqueue_global_config_path]()->unique_ptr<phxqueue::plugin::ConfigFactory> {
            auto cf = new phxqueue_phxrpc::plugin::ConfigFactory(phxqueue_global_config_path);
            return unique_ptr<phxqueue::plugin::ConfigFactory>(cf);
        });

    phxqueue::producer::StoreSelectorDefault selector(topic_id, pub_id, uin, retry_switch_store);
    int store_id = -1;
    for (int i = 0; i < 3; ++i) {
        phxqueue::comm::RetCode ret = selector.GetStoreID(store_id);
        printf("%d %d\n", as_integer(ret), store_id);
    }

    return 0;
}

