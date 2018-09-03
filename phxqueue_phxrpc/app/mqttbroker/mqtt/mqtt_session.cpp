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

#include "mqtt_session.h"

#include "phxrpc/network.h"


namespace phxqueue_phxrpc {

namespace mqttbroker {


using namespace std;


void MqttSession::Heartbeat() {
    if (0 >= keep_alive) {
        expire_time_ms_ = static_cast<uint64_t>(-1);
    } else {
        // mqtt-3.1.2-24: keep_alive * 1.5
        expire_time_ms_ = keep_alive * 1500 + phxrpc::Timer::GetSteadyClockMS();
    }
}

bool MqttSession::IsExpired() {
    return expire_time_ms_ <= phxrpc::Timer::GetSteadyClockMS();
}

uint64_t MqttSession::expire_time_ms() const {
    return expire_time_ms_;
}


MqttSessionMgr *MqttSessionMgr::GetInstance() {
    if (!s_instance) {
        s_instance.reset(new MqttSessionMgr);
    }

    return s_instance.get();
}

MqttSessionMgr::MqttSessionMgr() {
}

MqttSessionMgr::~MqttSessionMgr() {
}

MqttSession *MqttSessionMgr::Create(const string &client_id, const uint64_t session_id) {
    MqttSession session;
    session.client_id = client_id;
    session.session_id = session_id;

    lock_guard<mutex> lock(mutex_);

    auto kv(sessions_map_.emplace(session_id, move(session)));
    if (kv.second) {
        return &(kv.first->second);
    }

    return nullptr;
}

MqttSession *MqttSessionMgr::GetByClientId(const string &client_id) {
    lock_guard<mutex> lock(mutex_);

    for (auto &&kv : sessions_map_) {
        if (kv.second.client_id == client_id)
            return &(kv.second);
    }

    return nullptr;
}

MqttSession *MqttSessionMgr::GetBySessionId(const uint64_t session_id) {
    lock_guard<mutex> lock(mutex_);

    auto it(sessions_map_.find(session_id));
    if (sessions_map_.end() == it) {
        return nullptr;
    }

    return &(it->second);
}

void MqttSessionMgr::DestroyBySessionId(const uint64_t session_id) {
    lock_guard<mutex> lock(mutex_);

    auto it(sessions_map_.find(session_id));
    if (sessions_map_.end() != it) {
        sessions_map_.erase(it);
    }
}

int MqttSessionMgr::UpdateAckPos(const uint64_t session_id, const uint32_t packet_id) {
    lock_guard<mutex> guard(mutex_);

    auto it(sessions_map_.find(session_id));
    if (sessions_map_.end() == it) {
        return -1;
    }
    if (packet_id == it->second.acked_packet_id + 1) {
        ++(it->second.acked_packet_id);
    }

    return 0;
}

unique_ptr<MqttSessionMgr> MqttSessionMgr::s_instance;


}  // namespace mqttbroker

}  // namespace phxqueue_phxrpc

