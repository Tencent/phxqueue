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

#include "mqtt_packet_id.h"

#include "phxqueue/comm.h"


namespace phxqueue_phxrpc {

namespace mqttbroker {


using namespace std;


MqttPacketIdMgr *MqttPacketIdMgr::GetInstance() {
    if (!s_instance) {
        s_instance.reset(new MqttPacketIdMgr);
    }

    return s_instance.get();
}

MqttPacketIdMgr::MqttPacketIdMgr() {
}

MqttPacketIdMgr::~MqttPacketIdMgr() {
}

bool MqttPacketIdMgr::AllocPacketId(const uint64_t cursor_id, const string &pub_client_id,
                                    const uint16_t pub_packet_id, const string &sub_client_id,
                                    uint16_t *const sub_packet_id) {
    if (!sub_packet_id)
        return false;

    *sub_packet_id = 0u;
    lock_guard<mutex> lock(mutex_);

    auto &&sub_client_info(sub_client_id2sub_client_info_map_[sub_client_id]);

    // no available packet id to alloc
    if (sub_client_info.max_size() <= sub_client_info.size()) {
        return false;
    }

    // alloc
    PubInfo pub_info(cursor_id, pub_client_id, pub_packet_id);
    *sub_packet_id = sub_client_info.ModPos(sub_client_info.in_pos());
    sub_client_info.set_in_pos(sub_client_info.in_pos() + 1);
    if (0u == sub_client_info.ModPos(sub_client_info.in_pos())) {
        // 0 is forbidden
        sub_client_info.set_in_pos(1u);
    }
    (*sub_client_info.mutable_map()).emplace(*sub_packet_id, move(pub_info));
    printf("sub_packet_id %u map.size %zu\n", *sub_packet_id, (*sub_client_info.mutable_map()).size());

    return true;
}

bool MqttPacketIdMgr::ReleasePacketId(const string &sub_client_id, const uint16_t sub_packet_id) {
    lock_guard<mutex> lock(mutex_);

    auto &&sub_client_info(sub_client_id2sub_client_info_map_[sub_client_id]);

    uint16_t erase_size(sub_packet_id + 1 - sub_client_info.ModPos(sub_client_info.out_pos()));
    if (sub_client_info.size() < erase_size) {
        // range overflow
        return false;
    }

    for (size_t i{sub_client_info.out_pos()}; sub_client_info.out_pos() + erase_size > i; ++i) {
        auto it(sub_client_info.map().find(i));
        if (sub_client_info.map().end() !=it) {
            sub_client_info.set_prev_cursor_id(it->second.cursor_id);
            sub_client_info.mutable_map()->erase(it);
        }
    }
    sub_client_info.set_out_pos(sub_packet_id + 1);
    if (0u == sub_client_info.ModPos(sub_client_info.out_pos())) {
        // 0 is forbidden
        sub_client_info.set_out_pos(1u);
    }

    return true;
}

bool MqttPacketIdMgr::GetCursorId(const string &sub_client_id, const uint16_t sub_packet_id,
                                  uint64_t *const cursor_id) const {
    if (!cursor_id)
        return false;

    *cursor_id = static_cast<uint64_t>(-1);
    lock_guard<mutex> lock(mutex_);

    auto it(sub_client_id2sub_client_info_map_.find(sub_client_id));
    if (sub_client_id2sub_client_info_map_.end() == it) {
        return false;
    }

    auto it2(it->second.map().find(sub_packet_id));
    if (it->second.map().end() == it2) {
        return false;
    }

    *cursor_id = it2->second.cursor_id;

    return true;
}

string MqttPacketIdMgr::ToString(const string &sub_client_id) const {
    lock_guard<mutex> lock(mutex_);

    auto it(sub_client_id2sub_client_info_map_.find(sub_client_id));
    if (sub_client_id2sub_client_info_map_.end() == it) {
        return "";
    }

    string s("sub_client_id:");
    s += sub_client_id + ",in_pos:" + to_string(it->second.in_pos()) +
            ",out_pos:" + to_string(it->second.out_pos()) + ",map:{";
    for (const auto &kv : it->second.map()) {
        s += "sub_packet_id:";
        s += to_string(kv.first) + ",info:{";
        s += "cursor_id:";
        s += to_string(kv.second.cursor_id) + ",";
        s += "pub_client_id:";
        s += kv.second.pub_client_id + ",";
        s += "pub_packet_id:";
        s += to_string(kv.second.pub_packet_id) + "},";
    }
    s += "}";

    return s;
}

bool MqttPacketIdMgr::GetPrevCursorId(uint64_t *const prev_cursor_id) const {
    if (!prev_cursor_id)
        return false;

    *prev_cursor_id = static_cast<uint64_t>(-1);
    lock_guard<mutex> lock(mutex_);
    for (const auto &kv : sub_client_id2sub_client_info_map_) {
        if (static_cast<uint64_t>(-1) == *prev_cursor_id ||
            (static_cast<uint64_t>(-1) != kv.second.prev_cursor_id() &&
             kv.second.prev_cursor_id() > *prev_cursor_id)) {
            *prev_cursor_id = kv.second.prev_cursor_id();
        }
    }

    return true;
}

unique_ptr<MqttPacketIdMgr> MqttPacketIdMgr::s_instance;


}  // namespace mqttbroker

}  // namespace phxqueue_phxrpc

