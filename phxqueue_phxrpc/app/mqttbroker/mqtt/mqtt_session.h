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

#include <list>
#include <mutex>
#include <string>
#include <vector>


struct RetainMessage final {
    std::string topic_name;
    std::string content;
    uint32_t qos{0u};
};


class MqttSession {
  public:
    void Heartbeat();
    bool IsExpired();

    uint64_t expire_time_ms() const;

    uint64_t session_id;
    std::string client_id;
    uint32_t keep_alive{10};
    std::vector<RetainMessage> retain_messages;

  private:
    uint64_t expire_time_ms_{0uLL};
};


class MqttSessionMgr final {
  public:
    MqttSessionMgr();
    ~MqttSessionMgr();

    MqttSession *Create(const std::string &client_id, const uint64_t session_id);
    MqttSession *GetByClientId(const std::string &client_id);
    MqttSession *GetBySessionId(const uint64_t session_id);
    void DestroyBySessionId(const uint64_t session_id);

  private:
    std::list<MqttSession> sessions_;

    std::mutex mutex_;
};

