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


class CheckConfig {
  public:
    void Process();

  private:
    void CheckGlobalConfig(config::GlobalConfig *global_config);
    void CheckTopicConfig(const int topic_id, const config::TopicConfig *topic_config);
    void CheckConsumerConfig(const int topic_id, const config::ConsumerConfig *consumer_config);
    void CheckStoreConfig(const int topic_id, const config::StoreConfig *store_config);
    void CheckSchedulerConfig(const int topic_id, const config::SchedulerConfig *scheduler_config);
    void CheckLockConfig(const int topic_id, const config::LockConfig *lock_config);

};


}  // namespace test

}  // namespace phxqueue

