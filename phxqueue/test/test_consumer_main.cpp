/*
Tencent is pleased to support the open source community by making PhxQueue available.
Copyright (C) 2017 THL A29 Limited, a Tencent company. All rights reserved.
Licensed under the BSD 3-Clause License (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at

<https://opensource.org/licenses/BSD-3-Clause>

Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.
*/



#include <iostream>

#include "phxqueue/test/simpleconsumer.h"
#include "phxqueue/test/simplehandler.h"

#include "phxqueue/comm.h"
#include "phxqueue/plugin.h"


using namespace phxqueue;
using namespace std;


int main(int argc, char **argv) {
    comm::LogFunc log_func;
    plugin::LoggerGoogle::GetLogger("test_consumer", "/tmp/phxqueue/log", 3, log_func);

    consumer::ConsumerOption opt;
    opt.topic = "test";
    opt.ip = "127.0.0.1";
    opt.port = 8001;
    opt.nprocs = 3;
    opt.proc_pid_path = "/tmp/phxqueue/";
    opt.lock_path_base = "./phxqueueconsumer.lock.";
    opt.log_func = log_func;

    test::SimpleConsumer consumer(opt);
    consumer.AddHandlerFactory(1, new comm::DefaultHandlerFactory<test::SimpleHandler>());
    consumer.Run();

    return 0;
}

