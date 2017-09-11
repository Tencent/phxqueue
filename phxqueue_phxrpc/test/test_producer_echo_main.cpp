/*
Tencent is pleased to support the open source community by making PhxQueue available.
Copyright (C) 2017 THL A29 Limited, a Tencent company. All rights reserved.
Licensed under the BSD 3-Clause License (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at

<https://opensource.org/licenses/BSD-3-Clause>

Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.
*/



#include <iostream>
#include <signal.h>

#include "phxqueue_phxrpc/comm.h"
#include "phxqueue_phxrpc/plugin.h"
#include "phxqueue_phxrpc/producer.h"


using namespace std;


extern char *program_invocation_short_name;

void ShowUsage(const char *program) {
    printf("\n");
    printf("Usage: %s\n", program);
    printf("\n");

    exit(0);
}

void GenRandomString(char *s, const int len) {
    static const char alpha_num[] =
        "0123456789"
        "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
        "abcdefghijklmnopqrstuvwxyz";

    srand(time(nullptr));
    for (int i = 0; i < len; ++i) {
        s[i] = alpha_num[rand() % (sizeof(alpha_num) - 1)];
    }

    s[len] = '\0';
}

int main(int argc, char **argv) {
    const char *module_name{program_invocation_short_name};

    if (argc < 1) {
        ShowUsage(module_name);
    }

    const string global_config_path("./etc/globalconfig.conf");

    phxqueue::plugin::ConfigFactory::SetConfigFactoryCreateFunc(
            [global_config_path]()->unique_ptr<phxqueue::plugin::ConfigFactory> {
                return unique_ptr<phxqueue::plugin::ConfigFactory>(
                        new phxqueue_phxrpc::plugin::ConfigFactory(global_config_path));
            });

    constexpr uint32_t len{10};
    char random_str[len + 1]{'\0'};
    GenRandomString(random_str, len);
    string buf(random_str);

    phxqueue::producer::ProducerOption opt;
    unique_ptr<phxqueue::producer::Producer> producer;
    producer.reset(new phxqueue_phxrpc::producer::Producer(opt));
    producer->Init();

    const int topic_id{1000};
    const uint64_t uin{0};
    const int handle_id{2};
    const int pub_id{1};

    phxqueue::comm::RetCode ret{producer->Enqueue(topic_id, uin, handle_id, buf, pub_id)};

    if (phxqueue::comm::RetCode::RET_OK == ret) {
        printf("produce echo \"%s\" succeeded!\n", buf.c_str());
        fflush(stdout);
    } else {
        printf("produce echo \"%s\" failed return %d!\n", buf.c_str(), phxqueue::comm::as_integer(ret));
        fflush(stdout);
    }

    return 0;
}

