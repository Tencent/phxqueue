/*
Tencent is pleased to support the open source community by making PhxQueue available.
Copyright (C) 2017 THL A29 Limited, a Tencent company. All rights reserved.
Licensed under the BSD 3-Clause License (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at

<https://opensource.org/licenses/BSD-3-Clause>

Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.
*/



#pragma once

#include "phxqueue_phxrpc/app/logic/mqtt.h"

#include "circular_queue.hpp"


namespace phxqueue_phxrpc {

namespace mqttbroker {


typedef CircularQueue<uint64_t, logic::mqtt::HttpPublishPb> PublishQueueBase;

class PublishQueue : public PublishQueueBase {
  public:
    static PublishQueue *GetInstance();
    static void SetInstance(PublishQueue *const default_instance);

    PublishQueue(const size_t max_size);
    virtual ~PublishQueue() override = default;

  private:
    static std::unique_ptr<PublishQueue> s_instance;
};


typedef LruCache<uint64_t, logic::mqtt::HttpPublishPb> PublishLruCacheBase;

class PublishLruCache : public PublishLruCacheBase {
  public:
    static PublishLruCache *GetInstance();
    static void SetInstance(PublishLruCache *const default_instance);

    PublishLruCache(const size_t max_size);
    virtual ~PublishLruCache() override = default;

  private:
    static std::unique_ptr<PublishLruCache> s_instance;
};


//class PublishStateMgr {
//  public:
//    struct PublishState {
//        uint64_t publish_packet_id;
//        uint64_t puback_packet_id;
//    };
//
//    typedef std::map<std::string, PublishState> PublishStateMap;
//
//    static PublishStateMgr *GetInstance();
//
//    PublishStateMgr() = default;
//    virtual ~PublishStateMgr() = default;
//
//    std::pair<PublishStateMap::iterator, bool> CreateSubClientId(const std::string &sub_client_id);
//    void DestroySubClientId(const std::string &sub_client_id);
//
//    bool GetPublishPacketId(const std::string &sub_client_id,
//                            uint64_t *const publish_packet_id) const;
//    bool SetPublishPacketId(const std::string &sub_client_id,
//                            const uint64_t publish_packet_id);
//    bool GetPubackPacketId(const std::string &sub_client_id,
//                           uint64_t *const puback_packet_id) const;
//    bool SetPubackPacketId(const std::string &sub_client_id,
//                           const uint64_t puback_packet_id);
//
//  private:
//    PublishStateMap map_;
//};


}  // namespace mqttbroker

}  // namespace phxqueue_phxrpc

