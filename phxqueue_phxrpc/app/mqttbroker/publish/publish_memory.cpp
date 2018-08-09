/*
Tencent is pleased to support the open source community by making PhxQueue available.
Copyright (C) 2017 THL A29 Limited, a Tencent company. All rights reserved.
Licensed under the BSD 3-Clause License (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at

<https://opensource.org/licenses/BSD-3-Clause>

Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.
*/



#include "publish_memory.h"


namespace phxqueue_phxrpc {

namespace mqttbroker {


using namespace std;


unique_ptr<PublishQueue> PublishQueue::s_instance;

PublishQueue *PublishQueue::GetInstance() {
    return s_instance.get();
}

void PublishQueue::SetInstance(PublishQueue *const default_instance) {
    s_instance.reset(default_instance);
}

PublishQueue::PublishQueue(const size_t max_size) : PublishQueueBase(max_size) {
}


unique_ptr<PublishLruCache> PublishLruCache::s_instance;

PublishLruCache *PublishLruCache::GetInstance() {
    return s_instance.get();
}

void PublishLruCache::SetInstance(PublishLruCache *const default_instance) {
    s_instance.reset(default_instance);
}

PublishLruCache::PublishLruCache(const size_t max_size) : PublishLruCacheBase(max_size) {
}


PublishStateMgr *PublishStateMgr::GetInstance() {
    static thread_local std::unique_ptr<PublishStateMgr> instance;
    if (!instance) {
        instance.reset(new PublishStateMgr);
    }

    return instance.get();
}

pair<PublishStateMgr::PublishStateMap::iterator, bool> PublishStateMgr::CreateSubClientId(const string &sub_client_id) {
    return map_.emplace(sub_client_id, PublishState());
}

void PublishStateMgr::DestroySubClientId(const string &sub_client_id) {
    map_.erase(sub_client_id);
}

bool PublishStateMgr::GetPublishPacketId(const string &sub_client_id,
                                         uint64_t *const publish_packet_id) const {
    auto it(map_.find(sub_client_id));
    if (map_.end() == it) {
        return false;
    }

    *publish_packet_id = it->second.publish_packet_id;

    return true;
}

bool PublishStateMgr::SetPublishPacketId(const string &sub_client_id,
                                         const uint64_t publish_packet_id) {
    auto it(map_.find(sub_client_id));
    if (map_.end() == it) {
        auto kv(CreateSubClientId(sub_client_id));
        if (!kv.second) {
            return false;
        }

        it = kv.first;
    }

    it->second.publish_packet_id = publish_packet_id;

    return true;
}

bool PublishStateMgr::GetPubackPacketId(const string &sub_client_id,
                                        uint64_t *const puback_packet_id) const {
    auto it(map_.find(sub_client_id));
    if (map_.end() == it) {
        return false;
    }

    *puback_packet_id = it->second.puback_packet_id;

    return true;
}

bool PublishStateMgr::SetPubackPacketId(const string &sub_client_id,
                                        const uint64_t puback_packet_id) {
    auto it(map_.find(sub_client_id));
    if (map_.end() == it) {
        auto kv(CreateSubClientId(sub_client_id));
        if (!kv.second) {
            return false;
        }

        it = kv.first;
    }

    it->second.puback_packet_id = puback_packet_id;

    return true;
}


}  // namespace mqttbroker

}  // namespace phxqueue_phxrpc


