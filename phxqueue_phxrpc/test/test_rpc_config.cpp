/*
Tencent is pleased to support the open source community by making PhxQueue available.
Copyright (C) 2017 THL A29 Limited, a Tencent company. All rights reserved.
Licensed under the BSD 3-Clause License (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at

<https://opensource.org/licenses/BSD-3-Clause>

Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.
*/



#include "test_rpc_config.h"

#include <memory>

#include "phxqueue_phxrpc/config.h"
#include "phxqueue_phxrpc/plugin.h"


namespace phxqueue_phxrpc {

namespace test {


using namespace std;


void TestConfig::Process() {
    phxqueue::plugin::ConfigFactory::SetConfigFactoryCreateFunc(
            []()->unique_ptr<phxqueue::plugin::ConfigFactory> {
                return unique_ptr<phxqueue::plugin::ConfigFactory>(
                        new plugin::ConfigFactory("./etc/globalconfig.conf"));
            });

    const int topic_id{1000};

    // test topic_config
    {
        shared_ptr<const phxqueue::config::TopicConfig> topic_config;
        assert(phxqueue::comm::RetCode::RET_OK ==
               phxqueue::config::GlobalConfig::GetThreadInstance()->
               GetTopicConfigByTopicID(topic_id, topic_config));
        assert(topic_config != nullptr);

        TestTopicConfig(topic_config.get());
    }

    // test consumer_config
    {
        shared_ptr<const phxqueue::config::ConsumerConfig> consumer_config;
        assert(phxqueue::comm::RetCode::RET_OK ==
               phxqueue::config::GlobalConfig::GetThreadInstance()->
               GetConsumerConfig(topic_id, consumer_config));
        assert(consumer_config);

        TestConsumerConfig(consumer_config.get());
    }

    // test store_config
    {
        shared_ptr<const phxqueue::config::StoreConfig> store_config;
        assert(phxqueue::comm::RetCode::RET_OK ==
               phxqueue::config::GlobalConfig::GetThreadInstance()->
               GetStoreConfig(topic_id, store_config));
        assert(store_config != nullptr);

        TestStoreConfig(store_config.get());
    }

    // test scheduler_config
    {
        shared_ptr<const phxqueue::config::SchedulerConfig> scheduler_config;
        assert(phxqueue::comm::RetCode::RET_OK ==
               phxqueue::config::GlobalConfig::GetThreadInstance()->
               GetSchedulerConfig(topic_id, scheduler_config));
        assert(scheduler_config != nullptr);

        TestSchedulerConfig(scheduler_config.get());
    }

    // test lock_config
    {
        shared_ptr<const phxqueue::config::LockConfig> lock_config;
        assert(phxqueue::comm::RetCode::RET_OK ==
               phxqueue::config::GlobalConfig::GetThreadInstance()->
               GetLockConfig(topic_id, lock_config));
        assert(lock_config != nullptr);

        TestLockConfig(lock_config.get());
    }
}


}  // namespace test

}  // namespace phxqueue_phxrpc

