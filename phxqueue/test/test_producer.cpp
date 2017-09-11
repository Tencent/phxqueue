/*
Tencent is pleased to support the open source community by making PhxQueue available.
Copyright (C) 2017 THL A29 Limited, a Tencent company. All rights reserved.
Licensed under the BSD 3-Clause License (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at

<https://opensource.org/licenses/BSD-3-Clause>

Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.
*/



#include "phxqueue/test/simpleproducer.h"
#include "phxqueue/test/test_producer.h"

#include <iostream>
#include <memory>


namespace phxqueue {

namespace test {


using namespace std;


void TestProducer::Process() {
    producer::ProducerOption opt;

    test::SimpleProducer producer(opt);
    producer.Enqueue(1000, 123, 1, "buffer", 1);
}


}  // namespace test

}  // namespace phxqueue

