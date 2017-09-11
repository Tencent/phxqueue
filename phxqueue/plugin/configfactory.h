/*
Tencent is pleased to support the open source community by making PhxQueue available.
Copyright (C) 2017 THL A29 Limited, a Tencent company. All rights reserved.
Licensed under the BSD 3-Clause License (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at

<https://opensource.org/licenses/BSD-3-Clause>

Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.
*/



#pragma once

#include <memory>
#include <cassert>

#include "phxqueue/comm.h"
#include "phxqueue/config.h"


namespace phxqueue {

namespace plugin {


class ConfigFactory;
using ConfigFactoryCreateFunc = std::function<std::unique_ptr<ConfigFactory> ()>;

class ConfigFactory {
  public:
    ConfigFactory() {}
    virtual ~ConfigFactory() {}

    static void SetConfigFactoryCreateFunc(ConfigFactoryCreateFunc config_factory_create_func) {config_factory_create_func_ = config_factory_create_func;}

    static ConfigFactory *GetInstance() {
        static ConfigFactory *cf = nullptr;

        if (!cf) {
            if (config_factory_create_func_) cf = config_factory_create_func_().release();
            else cf = new ConfigFactory();
        }

        assert(cf);
        return cf;
    }

    virtual std::unique_ptr<config::GlobalConfig> NewGlobalConfig() {
        return std::unique_ptr<config::GlobalConfig>(new config::GlobalConfig());
    }
    virtual std::unique_ptr<config::TopicConfig> NewTopicConfig(const int topic_id, const std::string &path) {
        return std::unique_ptr<config::TopicConfig>(new config::TopicConfig());
    }
    virtual std::unique_ptr<config::ConsumerConfig> NewConsumerConfig(const int topic_id, const std::string &path) {
        return std::unique_ptr<config::ConsumerConfig>(new config::ConsumerConfig());
    }
    virtual std::unique_ptr<config::StoreConfig> NewStoreConfig(const int topic_id, const std::string &path) {
        return std::unique_ptr<config::StoreConfig>(new config::StoreConfig());
    }
    virtual std::unique_ptr<config::SchedulerConfig> NewSchedulerConfig(const int topic_id, const std::string &path) {
        return std::unique_ptr<config::SchedulerConfig>(new config::SchedulerConfig());
    }
    virtual std::unique_ptr<config::LockConfig> NewLockConfig(const int topic_id, const std::string &path) {
        return std::unique_ptr<config::LockConfig>(new config::LockConfig());
    }

  private:
    static ConfigFactoryCreateFunc config_factory_create_func_;
};


}  // namespace plugin

}  // namespace phxqueue

