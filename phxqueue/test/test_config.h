/*
Tencent is pleased to support the open source community by making PhxQueue available.
Copyright (C) 2017 THL A29 Limited, a Tencent company. All rights reserved.
Licensed under the BSD 3-Clause License (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at

<https://opensource.org/licenses/BSD-3-Clause>

Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.
*/



#pragma once

#include "phxqueue/config.h"


namespace phxqueue {

namespace test {


class TestConfig {
  public:
    static void TestGlobalConfig(config::GlobalConfig *global_config);
    static void TestTopicConfig(const config::TopicConfig *topic_config);
    static void TestConsumerConfig(const config::ConsumerConfig *consumer_config);
    static void TestStoreConfig(const config::StoreConfig *store_config);
    static void TestSchedulerConfig(const config::SchedulerConfig *scheduler_config);
    static void TestLockConfig(const config::LockConfig *lock_config);
};


}  // namespace test

}  // namespace phxqueue

