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

    return ret;
}

EventLoopServerConfig &MqttBrokerServerConfig::GetEventLoopServerConfig() {
    return event_loop_server_config_;
}

phxrpc::HshaServerConfig &MqttBrokerServerConfig::GetHshaServerConfig() {
    return hsha_server_config_;
}

int MqttBrokerServerConfig::GetTopicID() const {
    return topic_id_;
}

const char *MqttBrokerServerConfig::GetPhxQueueGlobalConfigPath() const {
    return phxqueue_global_config_path_;
}

