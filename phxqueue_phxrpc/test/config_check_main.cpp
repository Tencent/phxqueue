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
#include "phxqueue_phxrpc/comm.h"
#include "phxqueue_phxrpc/config.h"
#include "phxqueue_phxrpc/plugin.h"

#include "phxqueue/test/check_config.h"

using namespace std;


void ShowUsage(const char *proc) {
    printf("\t%s -f <global_config_path> [-d]\n", proc);
}

int main(int argc, const char *argv[]) {
    const char *config_path = nullptr;
    int debug = 0;

    for (int i = 0; i < argc; ++i) {
		if (strcmp(argv[i], "--help") == 0)
		{
			ShowUsage(argv[0]);
			return 0;
		} else if (strcmp(argv[i], "-f") == 0) {
            config_path = argv[++i];
            continue;
        } else if (strcmp(argv[i], "-d") == 0) {
            debug = 1;
            continue;
        }
    }


    if (!config_path) {
        ShowUsage(argv[0]);
        return 0;
    }

    phxqueue::comm::Logger::GetInstance()->SetLogFunc([debug](const int log_level, const char *format, va_list args)->void {
            if (!debug && log_level > static_cast<int>(phxqueue::comm::LogLevel::Error)) return;
            vprintf(format, args);
            printf("\n");
        });

    string phxqueue_global_config_path = config_path;
    phxqueue::plugin::ConfigFactory::SetConfigFactoryCreateFunc([phxqueue_global_config_path]()->unique_ptr<phxqueue::plugin::ConfigFactory> {
            auto cf = new phxqueue_phxrpc::plugin::ConfigFactory(phxqueue_global_config_path);
            cf->SetNeedCheck(true);
            return unique_ptr<phxqueue::plugin::ConfigFactory>(cf);
        });

    phxqueue::config::GlobalConfig::GetThreadInstance();

    phxqueue::test::CheckConfig check_config;
    check_config.Process();

    return 0;
}


