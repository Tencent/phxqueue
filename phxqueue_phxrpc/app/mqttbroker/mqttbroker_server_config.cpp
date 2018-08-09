/*
Tencent is pleased to support the open source community by making
PhxRPC available.
Copyright (C) 2016 THL A29 Limited, a Tencent company.
All rights reserved.

Licensed under the BSD 3-Clause License (the "License"); you may
not use this file except in compliance with the License. You may
obtain a copy of the License at

https://opensource.org/licenses/BSD-3-Clause

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" basis,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
implied. See the License for the specific language governing
permissions and limitations under the License.

See the AUTHORS file for names of contributors.
*/

#include "mqttbroker_server_config.h"

#include "mqttbroker.pb.h"


MqttBrokerServerConfig::MqttBrokerServerConfig() {
}

MqttBrokerServerConfig::~MqttBrokerServerConfig() {
}

bool MqttBrokerServerConfig::Read(const char *config_file) {
    bool ret{event_loop_server_config_.Read(config_file)};
    ret &= hsha_server_config_.Read(config_file);

    if (0 == strlen(event_loop_server_config_.GetPackageName())) {
        event_loop_server_config_.SetPackageName("phxqueue_phxrpc.mqttbroker");
    }

    if (0 == strlen(hsha_server_config_.GetPackageName())) {
        hsha_server_config_.SetPackageName("phxqueue_phxrpc.mqttbroker");
    }

    // read extra
    phxrpc::Config config;
    if (!config.InitConfig(config_file)) {
        return false;
    }
    ret &= config.ReadItem("MqttBroker", "TopicID", &topic_id_);
    ret &= config.ReadItem("MqttBroker", "PhxQueueGlobalConfigPath",
                           phxqueue_global_config_path_, sizeof(phxqueue_global_config_path_));
    config.ReadItem("MqttBroker", "MaxPublishQueueSize", &max_publish_queue_size_);
    config.ReadItem("MqttBroker", "MaxPublishLruCacheSize", &max_publish_lru_cache_size_);
    config.ReadItem("MqttBroker", "NrPublishThread", &nr_publish_thread_);
    config.ReadItem("MqttBroker", "ShareStackSizeKb", &share_stack_size_kb_);
    config.ReadItem("MqttBroker", "PublishSleepTimeMs", &publish_sleep_time_ms_);

    return ret;
}

EventLoopServerConfig &MqttBrokerServerConfig::GetEventLoopServerConfig() {
    return event_loop_server_config_;
}

phxrpc::HshaServerConfig &MqttBrokerServerConfig::GetHshaServerConfig() {
    return hsha_server_config_;
}

int MqttBrokerServerConfig::topic_id() const {
    return topic_id_;
}

const char *MqttBrokerServerConfig::phxqueue_global_config_path() const {
    return phxqueue_global_config_path_;
}

int MqttBrokerServerConfig::max_publish_queue_size() const {
    return max_publish_queue_size_;
}

int MqttBrokerServerConfig::max_publish_lru_cache_size() const {
    return max_publish_lru_cache_size_;
}

int MqttBrokerServerConfig::nr_publish_thread() const {
    return nr_publish_thread_;
}

int MqttBrokerServerConfig::share_stack_size_kb() const {
    return share_stack_size_kb_;
}

int MqttBrokerServerConfig::publish_sleep_time_ms() const {
    return publish_sleep_time_ms_;
}

