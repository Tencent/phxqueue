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

#pragma once

#include "event_loop_server_config.h"


class MqttBrokerServerConfig {
  public:
    MqttBrokerServerConfig();

    virtual ~MqttBrokerServerConfig();

    bool Read(const char *config_file);

    EventLoopServerConfig &GetEventLoopServerConfig();
    phxrpc::HshaServerConfig &GetHshaServerConfig();

    int topic_id() const;
    const char *phxqueue_global_config_path() const;
    int max_publish_queue_size() const;
    int max_publish_lru_cache_size() const;
    int nr_publish_thread() const;
    int share_stack_size_kb() const;
    int publish_sleep_time_ms() const;

  private:
    EventLoopServerConfig event_loop_server_config_;
    phxrpc::HshaServerConfig hsha_server_config_;

    int topic_id_{-1};
    char phxqueue_global_config_path_[128];
    int max_publish_queue_size_{128 * 1024 * 1024};
    int max_publish_lru_cache_size_{16 * 1024 * 1024};
    int nr_publish_thread_{2};
    int share_stack_size_kb_{128};
    int publish_sleep_time_ms_{10};
};

