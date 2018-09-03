/*
Tencent is pleased to support the open source community by making PhxQueue available.
Copyright (C) 2017 THL A29 Limited, a Tencent company. All rights reserved.
Licensed under the BSD 3-Clause License (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at

<https://opensource.org/licenses/BSD-3-Clause>

Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.
*/



#pragma once

#include <memory>
#include <queue>

#include "co_routine.h"

#include "phxrpc/rpc.h"


class MqttBrokerServerConfig;

namespace phxqueue_phxrpc {

namespace mqttbroker {


enum class SessionOpType {
    NOP = 0,
    CREATE = 1,
    DESTROY = 2,
    MAX = 3,
};


class SessionOpQueue {
  public:
    struct SessionOpItem {
        SessionOpItem(const uint64_t session_id_value, const SessionOpType op_value)
                : session_id(session_id_value), op(op_value) {}
        uint64_t session_id{0uLL};
        SessionOpType op{SessionOpType::NOP};
    };

    int Push(const uint64_t session_id, const SessionOpType op);
    int Pop(uint64_t *const session_id, SessionOpType *const op);

  private:
    std::queue<SessionOpItem> queue_;

    std::mutex mutex_;
};


class ServerMgr;

class PublishThread {
  public:
    PublishThread(const MqttBrokerServerConfig *const config);
    PublishThread(PublishThread &&publish_thread);
    virtual ~PublishThread();

    void RunFunc();

    int CreateSession(const uint64_t session_id);
    int DestroySession(const uint64_t session_id);
    int PopSessionOp(uint64_t *const session_id, SessionOpType *const op);

    const MqttBrokerServerConfig *config() { return config_; }

  private:
    const MqttBrokerServerConfig *config_{nullptr};
    SessionOpQueue session_op_queue_;

    std::thread thread_;
    // add member should also change move constructor
};


class PublishMgr {
  public:
    static PublishMgr *GetInstance();
    static void SetInstance(PublishMgr *const instance);

    PublishMgr(const MqttBrokerServerConfig *const config);
    virtual ~PublishMgr();

    int CreateSession(const uint64_t session_id);
    int DestroySession(const uint64_t session_id);

  private:
    static std::unique_ptr<PublishMgr> s_instance;

    const MqttBrokerServerConfig *config_{nullptr};
    size_t nr_publish_threads_{0u};
    std::vector<std::unique_ptr<PublishThread>> publish_threads_;
};


}  // namespace mqttbroker

}  // namespace phxqueue_phxrpc

