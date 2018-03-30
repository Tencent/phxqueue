/*
Tencent is pleased to support the open source community by making PhxQueue available.
Copyright (C) 2017 THL A29 Limited, a Tencent company. All rights reserved.
Licensed under the BSD 3-Clause License (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at

<https://opensource.org/licenses/BSD-3-Clause>

Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.
*/



#include <memory>
#include <functional>
#include <zlib.h>


#include "phxqueue/comm.h"
#include "phxqueue/config.h"

#include "phxqueue/producer/selector.h"


namespace phxqueue {

namespace producer {


using namespace std;


class QueueSelectorDefault::QueueSelectorDefaultImpl {
  public:
    int topic_id;
    int pub_id;
    uint64_t uin;
    int count;
    bool retry_switch_queue;
    int retry;
};

QueueSelectorDefault::QueueSelectorDefault(const int topic_id, const int pub_id, const uint64_t uin, const int count, const bool retry_switch_queue) : impl_(new QueueSelectorDefaultImpl()){
    assert(impl_);
    impl_->topic_id = topic_id;
    impl_->pub_id = pub_id;
    impl_->uin = uin;
    impl_->count = count;
    impl_->retry_switch_queue = retry_switch_queue;
    impl_->retry = 0;
}

QueueSelectorDefault::~QueueSelectorDefault() {
}

comm::RetCode QueueSelectorDefault::GetQueueID(const comm::proto::QueueType queue_type, int &queue_id) {
    comm::RetCode ret;

    std::shared_ptr<const config::TopicConfig> topic_config;
    if (comm::RetCode::RET_OK != (ret = config::GlobalConfig::GetThreadInstance()->GetTopicConfigByTopicID(impl_->topic_id, topic_config))) {
        QLErr("GetTopicConfigByTopicID ret %d", as_integer(ret));
        return ret;
    }

    int queue_info_id;
    if (comm::RetCode::RET_OK != (ret = topic_config->GetQueueInfoIDByCount(impl_->pub_id, impl_->count, queue_info_id, queue_type))) {
        QLErr("GetQueueInfoIDByCount ret %d", as_integer(ret));
        return ret;
    }

    shared_ptr<const config::proto::QueueInfo> queue_info;
    if (comm::RetCode::RET_OK != (ret = topic_config->GetQueueInfoByQueueInfoID(queue_info_id, queue_info))) {
        QLErr("GetQueueInfoByQueueInfoID ret %d", as_integer(ret));
        return ret;
    }

    size_t rd = crc32(0, Z_NULL, 0);
    {
        uint64_t tmp_uin = (!impl_->uin || queue_info->handle_by_random_uin()) ? comm::utils::OtherUtils::FastRand() : impl_->uin;
        rd = crc32(rd, (const unsigned char *)&tmp_uin, sizeof(uint64_t));
        rd = crc32(rd, (const unsigned char *)&impl_->topic_id, sizeof(int));
        if (impl_->retry_switch_queue) rd += impl_->retry;
    }

    if (0 > comm::as_integer(ret = topic_config->GetQueueByLoopRank(queue_info_id, rd, queue_id))) {
        QLErr("GetQueueByLoopRank ret %d queue_info_id %d rd %d", ret, queue_info_id, rd);
    }

    QLVerb("pub_id %d count %d queue_info_id %d rd %zu queue_id %d retry %d", impl_->pub_id, impl_->count, queue_info_id, rd, queue_id, impl_->retry);

    ++impl_->retry;

    return comm::RetCode::RET_OK;
}

class StoreSelectorDefault::StoreSelectorDefaultImpl {
  public:
    int topic_id;
    int pub_id;
    uint64_t uin;
    bool retry_switch_store;
    int retry;
    set<int> tried_store_ids;
};

StoreSelectorDefault::StoreSelectorDefault(const int topic_id, const int pub_id,
                                           const uint64_t uin, const bool retry_switch_store)
        : impl_(new StoreSelectorDefaultImpl()) {
    assert(impl_);
    impl_->topic_id = topic_id;
    impl_->pub_id = pub_id;
    impl_->uin = uin;
    impl_->retry_switch_store = retry_switch_store;
    impl_->retry = 0;
}

StoreSelectorDefault::~StoreSelectorDefault() {}

comm::RetCode StoreSelectorDefault::GetStoreID(int &store_id) {
    shared_ptr<const config::StoreConfig> store_config;

    comm::RetCode ret;
    if (comm::RetCode::RET_OK != (ret = config::GlobalConfig::GetThreadInstance()->GetStoreConfig(impl_->topic_id, store_config))) {
        QLErr("GetStoreConfig ret %d", as_integer(ret));
        return ret;
    }

    vector<shared_ptr<const config::proto::Store> > stores;
    if (comm::RetCode::RET_OK != (ret = store_config->GetAllStore(stores))) {
        QLErr("GetAllStore ret %d", as_integer(ret));
        return ret;
    }

    set<int> store_ids;
    for (auto &&store : stores) {
        set<int> pub_ids;
        if (comm::RetCode::RET_OK != (ret = config::utils::GetPubIDsByStoreID(impl_->topic_id, store->store_id(), pub_ids))) {
            QLErr("GetPubIDsByStoreID ret %d topic_id %d store_id %d", as_integer(ret), impl_->topic_id, store->store_id());
            continue;
        }
        if (pub_ids.end() != pub_ids.find(impl_->pub_id)) {
            store_ids.insert(store->store_id());
        }
    }

    if (store_ids.empty()) {
        QLErr("store_ids size 0");
        return comm::RetCode::RET_ERR_RANGE_STORE;
    }

    auto store_ids_size = store_ids.size();

    // rebuild consisten hash
    static __thread map<int, uint64_t> topic_id2last_rebuild_time;
    static __thread map<int, map<uint64_t, int>> topic_id2hash_ring;
    static __thread map<int, map<int, set<uint64_t>>> topic_id2store_id2hash_list;

    auto &hash_ring = topic_id2hash_ring[impl_->topic_id];
    auto &store_id2hash_list = topic_id2store_id2hash_list[impl_->topic_id];
    do {
        auto &last_rebuild_time = topic_id2last_rebuild_time[impl_->topic_id];

        auto last_mod_time = store_config->GetLastModTime();
        if (last_mod_time == last_rebuild_time) break;
        last_rebuild_time = last_mod_time;

        hash_ring.clear();
        store_id2hash_list.clear();

        for (auto &&tmp_store_id : store_ids) {
            shared_ptr<const config::proto::Store> store;
            if (comm::RetCode::RET_OK != (ret = store_config->GetStoreByStoreID(tmp_store_id, store))) {
                QLErr("GetStoreByStoreID ret %d topic_id %d store_id %d", as_integer(ret), impl_->topic_id, tmp_store_id);
                continue;
            }

            vector<uint64_t> encoded_addrs;
            for (int i{0}; i < store->addrs_size(); ++i) {
                encoded_addrs.push_back(comm::utils::EncodeAddr(store->addrs(i)));
            }

            auto &hash_list = store_id2hash_list[tmp_store_id];

            for (int i{0}; i < store->scale(); ++i) {
                size_t h = 0;
                for (auto &&encoded_addr : encoded_addrs) {
                    h = comm::utils::MurmurHash64(&encoded_addr, sizeof(uint64_t), h);
                }
                h = comm::utils::MurmurHash64(&i, sizeof(int), h);

                hash_ring.emplace(h, tmp_store_id);
                hash_list.insert(h);
            }
        }

    } while (0);

    // find store_id in consistent hash
    store_id = -1;
    while (-1 == store_id) {
        size_t h = 0;
        {
            uint64_t tmp_uin = !impl_->uin ? comm::utils::OtherUtils::FastRand() : impl_->uin;
            h = comm::utils::MurmurHash64(&tmp_uin, sizeof(uint64_t), h);
            h = comm::utils::MurmurHash64(&impl_->topic_id, sizeof(int), h);
            if (impl_->retry_switch_store) h = comm::utils::MurmurHash64(&impl_->retry, sizeof(int), h);
        }

        if (impl_->retry_switch_store && impl_->retry) {
            uint64_t min_hash = 0;
            for (auto &&tmp_store_id : store_ids) {
                if (impl_->tried_store_ids.end() != impl_->tried_store_ids.find(tmp_store_id)) continue;
                auto &hash_list = store_id2hash_list[tmp_store_id];

                auto &&it = hash_list.lower_bound(h);
                if (hash_list.end() == it) it = hash_list.begin();
                if (hash_list.end() != it) {
                    if (-1 == store_id || *it < min_hash) {
                        store_id = tmp_store_id;
                        min_hash = *it;
                    }
                }
            }
        } else {
            auto &&it = hash_ring.lower_bound(h);
            if (hash_ring.end() == it) it = hash_ring.begin();
            store_id = it->second;
        }

        ++impl_->retry;

        if (-1 != store_id) {
            if (impl_->retry_switch_store) {
                impl_->tried_store_ids.insert(store_id);
                if (IsStoreBlocked(store_id)) store_id = -1;
            }
        } else {
            auto &&it = store_ids.begin();
            advance(it, h % store_ids_size);
            store_id = *it;
        }
    }

    return comm::RetCode::RET_OK;
}

int StoreSelectorDefault::GetTopicID() {
    return impl_->topic_id;
}

int StoreSelectorDefault::GetPubID() {
    return impl_->pub_id;
}

uint64_t StoreSelectorDefault::GetUin() {
    return impl_->uin;
}

bool StoreSelectorDefault::IsRetrySwitchStore() {
    return impl_->retry_switch_store;
}

}  // namespace producer

}  // namespace phxqueue


//gzrd_Lib_CPP_Version_ID--start
#ifndef GZRD_SVN_ATTR
#define GZRD_SVN_ATTR "0"
#endif
static char gzrd_Lib_CPP_Version_ID[] __attribute__((used))="$HeadURL$ $Id$ " GZRD_SVN_ATTR "__file__";
// gzrd_Lib_CPP_Version_ID--end

