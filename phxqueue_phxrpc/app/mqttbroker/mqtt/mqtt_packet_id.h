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

#include <bitset>
#include <map>
#include <memory>
#include <mutex>
#include <string>


class MqttPacketIdMgr final {
  public:
    MqttPacketIdMgr();
    ~MqttPacketIdMgr();

    bool AllocPacketId(const std::string &pub_client_id, const uint16_t pub_packet_id,
                       const std::string &sub_client_id, uint16_t &sub_packet_id);
    bool ReleasePacketId(const std::string &pub_client_id, const uint16_t pub_packet_id,
                         const std::string &sub_client_id);
    bool TestPacketId(const std::string &pub_client_id, const uint16_t pub_packet_id,
                      const std::string &sub_client_id);

  private:
    std::map<std::string, std::bitset<65536>> sub_client_id2sub_packet_ids_map_;
    std::map<std::string, std::map<std::string, uint16_t>> sub_client_id2pub_keys_map_;
    uint16_t current_sub_packet_id_{static_cast<uint16_t>(0)};

    std::mutex mutex_;
};

