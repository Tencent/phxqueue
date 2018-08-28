/*
Tencent is pleased to support the open source community by making PhxQueue available.
Copyright (C) 2017 THL A29 Limited, a Tencent company. All rights reserved.
Licensed under the BSD 3-Clause License (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at

<https://opensource.org/licenses/BSD-3-Clause>

Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.
*/



#pragma once

#include <memory>

#include "co_routine.h"

#include "phxrpc/rpc.h"


class MqttBrokerServerConfig;

namespace phxqueue_phxrpc {

namespace mqttbroker {


class ServerMgr;


class PublishThread {
  public:
    PublishThread(const MqttBrokerServerConfig *const config, ServerMgr *const server_mgr);
    PublishThread(PublishThread &&publish_thread);
    virtual ~PublishThread();

    void RunFunc();

    int CreateSession(const uint64_t session_id);
    int DestroySession(const uint64_t session_id);

  private:
    const MqttBrokerServerConfig *config_{nullptr};
    ServerMgr *server_mgr_{nullptr};

    std::thread thread_;
    // add member should also change move constructor
};


class PublishMgr {
  public:
    PublishMgr(const MqttBrokerServerConfig *const config, ServerMgr *const server_mgr);
    virtual ~PublishMgr();

    int CreateSession(const uint64_t session_id);
    int DestroySession(const uint64_t session_id);

  private:
    std::vector<std::unique_ptr<PublishThread>> publish_threads_;
};


}  // namespace mqttbroker

}  // namespace phxqueue_phxrpc

