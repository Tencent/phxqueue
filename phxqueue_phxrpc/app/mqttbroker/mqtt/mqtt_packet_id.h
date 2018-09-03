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


namespace phxqueue_phxrpc {

namespace mqttbroker {


class MqttPacketIdMgr final {
  public:
    struct PubInfo {
        PubInfo(const uint64_t cursor_id_value, const std::string &pub_client_id_value,
                const uint16_t pub_packet_id_value) : cursor_id(cursor_id_value),
                pub_client_id(pub_client_id_value), pub_packet_id(pub_packet_id_value) {
        }

        uint64_t cursor_id{0uLL};
        std::string pub_client_id;
        uint16_t pub_packet_id{0u};
    };

    typedef std::map<uint16_t, PubInfo> SubPacketId2PubInfoMap;

    class SubClientInfo {
      public:
        inline uint16_t size() const {
            return static_cast<uint16_t>(in_pos_ - out_pos_);
        }

        inline uint16_t ModPos(const size_t pos) const {
            return static_cast<uint16_t>(pos & (max_size_ - 1));  // pos % max_size
        }

        inline size_t max_size() const { return max_size_; }

        inline size_t in_pos() const { return in_pos_; }
        inline void set_in_pos(size_t pos) { in_pos_ = pos; }

        inline size_t out_pos() const { return out_pos_; }
        inline void set_out_pos(size_t pos) { out_pos_ = pos; }

        inline uint64_t prev_cursor_id() const { return prev_cursor_id_; }
        inline void set_prev_cursor_id(uint64_t cursor_id) { prev_cursor_id_ = cursor_id; }

        inline const SubPacketId2PubInfoMap &map() const {
            return map_;
        };

        inline SubPacketId2PubInfoMap *mutable_map() {
            return &map_;
        };

      private:
        const size_t max_size_{65536u};
        size_t in_pos_{1u};
        size_t out_pos_{1u};
        uint64_t prev_cursor_id_{static_cast<uint64_t>(-1)};
        SubPacketId2PubInfoMap map_;
    };

    static MqttPacketIdMgr *GetInstance();

    MqttPacketIdMgr();
    ~MqttPacketIdMgr();

    bool AllocPacketId(const uint64_t cursor_id, const std::string &pub_client_id,
                       const uint16_t pub_packet_id, const std::string &sub_client_id,
                       uint16_t *const sub_packet_id);
    bool ReleasePacketId(const std::string &sub_client_id, const uint16_t sub_packet_id);
    bool GetPrevCursorId(const std::string &sub_client_id, uint64_t *const prev_cursor_id);
    bool SetPrevCursorId(const std::string &sub_client_id, const uint64_t prev_cursor_id);
    //bool GetCursorIdByPacketId(const std::string &sub_client_id, const uint16_t sub_packet_id,
    //                           uint64_t *const cursor_id) const;
    std::string ToString(const std::string &sub_client_id) const;

    bool GetPrevCursorId(uint64_t *const prev_cursor_id) const;

  private:
    static std::unique_ptr<MqttPacketIdMgr> s_instance;

    std::map<std::string, SubClientInfo> sub_client_id2sub_client_info_map_;

    mutable std::mutex mutex_;
};


}  // namespace mqttbroker

}  // namespace phxqueue_phxrpc

