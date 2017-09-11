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

comm::RetCode QueueSelectorDefault::GetQueueID(int &queue_id) {
    comm::RetCode ret;

    std::shared_ptr<const config::TopicConfig> topic_config;
    if (comm::RetCode::RET_OK != (ret = config::GlobalConfig::GetThreadInstance()->GetTopicConfigByTopicID(impl_->topic_id, topic_config))) {
        QLErr("GetTopicConfigByTopicID ret %d", as_integer(ret));
        return ret;
    }

    int queue_info_id;
    if (comm::RetCode::RET_OK != (ret = topic_config->GetQueueInfoIDByCount(impl_->pub_id, impl_->count, queue_info_id))) {
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

    {
        size_t rd = crc32(0, Z_NULL, 0);
        {
            uint64_t tmp_uin = !impl_->uin ? comm::utils::OtherUtils::FastRand() : impl_->uin;
            rd = crc32(rd, (const unsigned char *)&tmp_uin, sizeof(uint64_t));
            rd = crc32(rd, (const unsigned char *)&impl_->topic_id, sizeof(int));
            if (impl_->retry_switch_store) rd = crc32(rd, (const unsigned char *)&impl_->retry, sizeof(int));
        }

        auto store_idx = rd % store_ids.size();
        auto it = store_ids.begin();
        advance(it, store_idx);
        store_id = *it;
    }

    ++impl_->retry;

    return comm::RetCode::RET_OK;
}


}  // namespace producer

}  // namespace phxqueue

