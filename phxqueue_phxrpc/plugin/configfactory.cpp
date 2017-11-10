/*
Tencent is pleased to support the open source community by making PhxQueue available.
Copyright (C) 2017 THL A29 Limited, a Tencent company. All rights reserved.
Licensed under the BSD 3-Clause License (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at

<https://opensource.org/licenses/BSD-3-Clause>

Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.
*/



#include "phxqueue_phxrpc/plugin/configfactory.h"

#include <string>

#include "phxqueue/comm.h"
#include "phxqueue_phxrpc/config.h"


namespace phxqueue_phxrpc {

namespace plugin {


using namespace std;


class ConfigFactory::ConfigFactoryImpl {
  public:
    ConfigFactoryImpl() {}
    virtual ~ConfigFactoryImpl() {}

    string global_config_path;
    bool need_check = false;
};

ConfigFactory::ConfigFactory(const string &global_config_path)
        : phxqueue::plugin::ConfigFactory(), impl_(new ConfigFactoryImpl()) {
    impl_->global_config_path = global_config_path;
}

ConfigFactory::~ConfigFactory() {}

unique_ptr<phxqueue::config::GlobalConfig> ConfigFactory::NewGlobalConfig() {
    QLVerb("path %s", impl_->global_config_path.c_str());

    auto &&conf(new config::GlobalConfig());
    conf->SetNeedCheck(impl_->need_check);
    conf->SetFileIfUnset(impl_->global_config_path);
    if (conf) conf->LoadIfModified();

    return unique_ptr<config::GlobalConfig>(conf);
}

unique_ptr<phxqueue::config::TopicConfig>
ConfigFactory::NewTopicConfig(const int topic_id, const string &path) {
    QLVerb("topic_id %d path %s", topic_id, path.c_str());

    auto &&conf(new config::TopicConfig());
    conf->SetNeedCheck(impl_->need_check);
    conf->SetFileIfUnset(path);
    if (conf) conf->LoadIfModified();

    return unique_ptr<config::TopicConfig>(conf);
}

unique_ptr<phxqueue::config::ConsumerConfig>
ConfigFactory::NewConsumerConfig(const int topic_id, const string &path) {
    QLVerb("topic_id %d path %s", topic_id, path.c_str());

    auto &&conf(new config::ConsumerConfig());
    conf->SetNeedCheck(impl_->need_check);
    conf->SetFileIfUnset(path);
    if (conf) conf->LoadIfModified();

    return unique_ptr<config::ConsumerConfig>(conf);
}

unique_ptr<phxqueue::config::StoreConfig>
ConfigFactory::NewStoreConfig(const int topic_id, const string &path) {
    QLVerb("topic_id %d path %s", topic_id, path.c_str());

    auto &&conf(new config::StoreConfig());
    conf->SetNeedCheck(impl_->need_check);
    conf->SetFileIfUnset(path);
    if (conf) conf->LoadIfModified();

    return unique_ptr<config::StoreConfig>(conf);
}

unique_ptr<phxqueue::config::SchedulerConfig>
ConfigFactory::NewSchedulerConfig(const int topic_id, const string &path) {
    QLVerb("topic_id %d path %s", topic_id, path.c_str());

    auto &&conf(new config::SchedulerConfig());
    conf->SetNeedCheck(impl_->need_check);
    conf->SetFileIfUnset(path);
    if (conf) conf->LoadIfModified();

    return unique_ptr<config::SchedulerConfig>(conf);
}

unique_ptr<phxqueue::config::LockConfig>
ConfigFactory::NewLockConfig(const int topic_id, const string &path) {
    QLVerb("topic_id %d path %s", topic_id, path.c_str());

    auto &&conf(new config::LockConfig());
    conf->SetNeedCheck(impl_->need_check);
    conf->SetFileIfUnset(path);
    if (conf) conf->LoadIfModified();

    return unique_ptr<config::LockConfig>(conf);
}

void ConfigFactory::SetNeedCheck(bool need_check) {
    impl_->need_check = need_check;
}


}  // namespace plugin

}  // namespace phxqueue_phxrpc

