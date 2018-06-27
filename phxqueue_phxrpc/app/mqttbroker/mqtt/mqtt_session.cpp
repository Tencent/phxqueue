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


using namespace std;


void MqttSession::Heartbeat() {
    if (0 >= keep_alive) {
        expire_time_ms_ = -1;
    } else {
        // mqtt-3.1.2-24: keep_alive * 1.5
        expire_time_ms_ = keep_alive * 1500 + phxrpc::Timer::GetSteadyClockMS();
    }
}

bool MqttSession::IsExpired() {
    return expire_time_ms_ <= phxrpc::Timer::GetSteadyClockMS();
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
    sessions_.emplace_back(move(session));

    return &(sessions_.back());
}

MqttSession *MqttSessionMgr::GetByClientId(const string &client_id) {
    lock_guard<mutex> lock(mutex_);
    for (auto &&session : sessions_) {
        if (session.client_id == client_id)
            return &session;
    }

    return nullptr;
}

MqttSession *MqttSessionMgr::GetBySessionId(const uint64_t session_id) {
    lock_guard<mutex> lock(mutex_);
    for (auto &&session : sessions_) {
        if (session.session_id == session_id)
            return &session;
    }

    return nullptr;
}

void MqttSessionMgr::DestroyBySessionId(const uint64_t session_id) {
    lock_guard<mutex> lock(mutex_);
    for (auto it(sessions_.begin()); sessions_.end() != it; ++it) {
        if (it->session_id == session_id) {
            sessions_.erase(it);

            return;
        }
    }
}

