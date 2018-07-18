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


using namespace std;


MqttPacketIdMgr::MqttPacketIdMgr() {
}

MqttPacketIdMgr::~MqttPacketIdMgr() {
}

bool MqttPacketIdMgr::AllocPacketId(const std::string &pub_client_id, const uint16_t pub_packet_id,
                                    const std::string &sub_client_id, uint16_t &sub_packet_id) {
    sub_packet_id = 0;
    lock_guard<mutex> lock(mutex_);
    auto &&sub_packet_ids(sub_client_id2sub_packet_ids_map_[sub_client_id]);

    size_t i{0u};
    while (true) {
        if (sub_packet_ids.size() > i) {
            ++i;
        } else {
            return false;
        }

        ++current_sub_packet_id_;
        if (sub_packet_ids.size() <= current_sub_packet_id_) {
            // 0 is forbidden
            current_sub_packet_id_ = 1;
        }
        if (!sub_packet_ids.test(current_sub_packet_id_)) {
            // alloc
            sub_packet_ids.set(current_sub_packet_id_);
            const string pub_key(phxqueue::comm::GetRouteKey(pub_client_id, pub_packet_id));
            sub_client_id2pub_keys_map_[sub_client_id][pub_key] = current_sub_packet_id_;
            sub_packet_id = current_sub_packet_id_;

            return true;
        }
    }

    return false;
}

bool MqttPacketIdMgr::ReleasePacketId(const string &pub_client_id, const uint16_t pub_packet_id,
                                      const string &sub_client_id) {
    lock_guard<mutex> lock(mutex_);
    auto sub_client_id2pub_keys_map_it(sub_client_id2pub_keys_map_.find(sub_client_id));
    if (sub_client_id2pub_keys_map_.end() == sub_client_id2pub_keys_map_it) {
        return false;
    }

    auto &&pub_keys_map(sub_client_id2pub_keys_map_it->second);
    const string pub_key(phxqueue::comm::GetRouteKey(pub_client_id, pub_packet_id));
    auto pub_keys_map_it(pub_keys_map.find(pub_key));
    if (pub_keys_map.end() == pub_keys_map_it) {
        return false;
    }

    auto sub_client_id2packet_ids_map_it(sub_client_id2sub_packet_ids_map_.find(sub_client_id));
    if (sub_client_id2sub_packet_ids_map_.end() == sub_client_id2packet_ids_map_it) {
        return false;
    }

    sub_client_id2packet_ids_map_it->second.reset(pub_keys_map_it->second);
    pub_keys_map.erase(pub_keys_map_it);

    return true;
}

bool MqttPacketIdMgr::TestPacketId(const string &pub_client_id, const uint16_t pub_packet_id,
                                   const string &sub_client_id) {
    lock_guard<mutex> lock(mutex_);
    auto sub_client_id2pub_keys_map_it(sub_client_id2pub_keys_map_.find(sub_client_id));
    if (sub_client_id2pub_keys_map_.end() == sub_client_id2pub_keys_map_it) {
        return false;
    }

    auto &&pub_keys_map(sub_client_id2pub_keys_map_it->second);
    const string pub_key(phxqueue::comm::GetRouteKey(pub_client_id, pub_packet_id));
    auto pub_keys_map_it(pub_keys_map.find(pub_key));
    if (pub_keys_map.end() == pub_keys_map_it) {
        return false;
    }

    auto sub_client_id2packet_ids_map_it(sub_client_id2sub_packet_ids_map_.find(sub_client_id));
    if (sub_client_id2sub_packet_ids_map_.end() == sub_client_id2packet_ids_map_it) {
        return false;
    }

    return sub_client_id2packet_ids_map_it->second.test(pub_keys_map_it->second);
}

