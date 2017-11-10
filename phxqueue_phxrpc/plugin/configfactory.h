/*
Tencent is pleased to support the open source community by making PhxQueue available.
Copyright (C) 2017 THL A29 Limited, a Tencent company. All rights reserved.
Licensed under the BSD 3-Clause License (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at

<https://opensource.org/licenses/BSD-3-Clause>

Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.
*/



#pragma once

#include <memory>

#include "phxqueue/config.h"
#include "phxqueue/plugin.h"


namespace phxqueue_phxrpc {

namespace plugin {


class ConfigFactory : public phxqueue::plugin::ConfigFactory {
  public:
    ConfigFactory(const std::string &global_config_path);
    virtual ~ConfigFactory();

    virtual std::unique_ptr<phxqueue::config::GlobalConfig> NewGlobalConfig();
    virtual std::unique_ptr<phxqueue::config::TopicConfig>
    NewTopicConfig(const int topic_id, const std::string &path);
    virtual std::unique_ptr<phxqueue::config::ConsumerConfig>
    NewConsumerConfig(const int topic_id, const std::string &path);
    virtual std::unique_ptr<phxqueue::config::StoreConfig>
    NewStoreConfig(const int topic_id, const std::string &path);
    virtual std::unique_ptr<phxqueue::config::SchedulerConfig>
    NewSchedulerConfig(const int topic_id, const std::string &path);
    virtual std::unique_ptr<phxqueue::config::LockConfig>
    NewLockConfig(const int topic_id, const std::string &path);

  public:
    void SetNeedCheck(bool need_check);

  private:
    class ConfigFactoryImpl;
    std::unique_ptr<ConfigFactoryImpl> impl_;
};


}  // namespace plugin

}  // namespace phxqueue_phxrpc

