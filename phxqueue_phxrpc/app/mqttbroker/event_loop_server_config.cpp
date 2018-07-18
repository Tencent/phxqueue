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

#include "event_loop_server_config.h"


EventLoopServerConfig::EventLoopServerConfig() {
    set_section_name_prefix("EventLoop");
}

EventLoopServerConfig::~EventLoopServerConfig() {
}

int EventLoopServerConfig::keep_alive_timeout_ms() const {
    return keep_alive_timeout_ms_;
}

void EventLoopServerConfig::set_keep_alive_timeout_ms(int keep_alive_timeout_ms) {
    keep_alive_timeout_ms_ = keep_alive_timeout_ms;
}

bool EventLoopServerConfig::DoRead(phxrpc::Config &config) {
    if (!HshaServerConfig::DoRead(config))
        return false;

    char server_timeout_section_name[128]{'\0'};
    strncpy(server_timeout_section_name, section_name_prefix(), sizeof(server_timeout_section_name) - 1);
    strncat(server_timeout_section_name, "ServerTimeout",
            sizeof(server_timeout_section_name) - sizeof(section_name_prefix()) - 1);
    config.ReadItem(server_timeout_section_name, "KeepAliveTimeoutMS", &keep_alive_timeout_ms_, 180000);

    return true;
}

