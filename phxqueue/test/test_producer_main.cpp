/*
Tencent is pleased to support the open source community by making PhxQueue available.
Copyright (C) 2017 THL A29 Limited, a Tencent company. All rights reserved.
Licensed under the BSD 3-Clause License (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at

<https://opensource.org/licenses/BSD-3-Clause>

Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.
*/



#include <iostream>

#include "phxqueue/comm.h"
#include "phxqueue/plugin.h"

#include "phxqueue/test/simpleproducer.h"


using namespace phxqueue;
using namespace std;


int main(int argc, char ** argv) {
    comm::LogFunc log_func;
    plugin::LoggerGoogle::GetLogger("test_producer", "/tmp/phxqueue/log", 3, log_func);

    producer::ProducerOption opt;
    opt.log_func = log_func;

    test::SimpleProducer producer(opt);
    producer.Init();
    producer.Enqueue(1000, 123, 1, "buffer", 1);

    return 0;
}

