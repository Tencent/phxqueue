/*
Tencent is pleased to support the open source community by making PhxQueue available.
Copyright (C) 2017 THL A29 Limited, a Tencent company. All rights reserved.
Licensed under the BSD 3-Clause License (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at

<https://opensource.org/licenses/BSD-3-Clause>

Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.
*/



#include "phxqueue/producer/producer.h"

#include <cinttypes>
#include <functional>
#include <zlib.h>

#include "phxqueue/comm.h"
#include "phxqueue/config.h"
#include "phxqueue/store.h"

#include "phxqueue/producer/batchhelper.h"



namespace phxqueue {

namespace producer {


using namespace std;


class Producer::ProducerImpl {
  public:
    ProducerOption opt;
    unique_ptr<BatchHelper> batch_helper = nullptr;
};

Producer::Producer(const ProducerOption &opt) : impl_(new ProducerImpl()) {
    assert(impl_);
    impl_->opt = opt;
}

Producer::~Producer() {
}

const ProducerOption * Producer::GetProducerOption() const {
    return &impl_->opt;
}

comm::RetCode Producer::Init() {
    if (impl_->opt.log_func) {
        comm::Logger::GetInstance()->SetLogFunc(impl_->opt.log_func);
    }

    if (impl_->opt.config_factory_create_func) {
        plugin::ConfigFactory::SetConfigFactoryCreateFunc(impl_->opt.config_factory_create_func);
    }

    if (impl_->opt.break_point_factory_create_func) {
        plugin::BreakPointFactory::SetBreakPointFactoryCreateFunc(impl_->opt.break_point_factory_create_func);
    }

    if (impl_->opt.ndaemon_batch_thread > 0) {
        impl_->batch_helper.reset(new BatchHelper(this));
        impl_->batch_helper->Init();
        impl_->batch_helper->Run();
    }

    return comm::RetCode::RET_OK;
}

static uint64_t SubIDs2Mask(const config::TopicConfig *topic_config, const set<int> *sub_ids) {
    uint64_t mask = -1;
    if (sub_ids) {
        mask = 0;
        int sub_id;
        for (auto &&it : *sub_ids) {
            sub_id = it;
            if (sub_id > 0) mask |= (1ULL << (sub_id - 1ULL));
        }
    }
    return mask;
}


comm::RetCode Producer::Enqueue(const int topic_id, const uint64_t uin, const int handle_id, const string &buffer,
                          int pub_id, const set<int> *sub_ids) {

    comm::RetCode ret;

    shared_ptr<const config::TopicConfig> topic_config;
    if (comm::RetCode::RET_OK != (ret = config::GlobalConfig::GetThreadInstance()->GetTopicConfigByTopicID(topic_id, topic_config))) {
        QLErr("GetTopicConfigByTopicID ret %d", as_integer(ret));
        return ret;
    }

    if (-1 == pub_id) DecidePubIDOnEnqueue(topic_id, uin, handle_id, pub_id);
    if (-1 == pub_id || !topic_config->IsValidPubID(pub_id)) {
        QLErr("pub_id %d not decide or invalid", pub_id);
        return comm::RetCode::RET_ERR_RANGE_PUB;
    }

    QLVerb("Enqueue. topic_id %d uin %" PRIu64 " buffer.length() %d pub_id %d handle_id %d", topic_id, uin, buffer.length(), pub_id, handle_id);

    comm::ProducerBP::GetThreadInstance()->OnEnqueue(topic_id, pub_id, handle_id, uin);
    comm::ProducerSubBP::GetThreadInstance()->OnSubDistribute(topic_id, pub_id, handle_id, uin, sub_ids);


    auto now = comm::utils::Time::GetTimestampMS();

    auto item = make_shared<comm::proto::QItem>();
    auto meta = item->mutable_meta();

    item->set_buffer(buffer);

    int buffer_type;
    CompressBuffer(buffer, *item->mutable_buffer(), buffer_type);
    item->set_buffer_type(buffer_type);

    item->set_sub_ids(SubIDs2Mask(topic_config.get(), sub_ids));
    item->set_pub_id(pub_id);
    item->set_atime(now / 1000);
    item->set_atime_ms(now % 1000);
    item->set_count(0);
    SetSysCookies(*item->mutable_sys_cookies());
    item->set_cursor_id(-1);


    meta->set_topic_id(topic_id);
    meta->set_handle_id(handle_id);
    meta->set_uin(uin);
    meta->set_sub_ids(item->sub_ids());
    meta->set_pub_id(item->pub_id());

    {
        size_t h = crc32(0, Z_NULL, 0);
        h = crc32(h, (const unsigned char *)buffer.c_str(), buffer.length());
        h = crc32(h, (const unsigned char *)&uin, sizeof(uint64_t));
        h = crc32(h, (const unsigned char *)&now, sizeof(uint64_t));
        meta->set_hash(h);
    }

    meta->set_atime(item->atime());
    meta->set_atime_ms(item->atime_ms());
    SetUserCookies(*meta->mutable_user_cookies());

    vector<shared_ptr<comm::proto::QItem> > items;
    items.emplace_back(move(item));
    QLVerb("item topic_id %d uin %" PRIu64, topic_id, uin);

    vector<unique_ptr<comm::proto::AddRequest> > reqs;
    if (comm::RetCode::RET_OK != (ret = MakeAddRequests(topic_id, items, reqs))) {
        QLErr("MakeAddRequests ret %d", as_integer(ret));
        return ret;
    }

    for (auto &&req : reqs) {
        if (comm::RetCode::RET_OK != (ret = SelectAndAdd(*req, nullptr, nullptr))) {
            comm::ProducerBP::GetThreadInstance()->OnSelectAndAddFail(topic_id, pub_id, handle_id, uin);
            QLErr("SelectAndAdd ret %d", as_integer(ret));
            return ret;
        }
    }

    comm::ProducerBP::GetThreadInstance()->OnEnqueueSucc(topic_id, pub_id, handle_id, uin);

    QLVerb("Enqueue succ");

    return comm::RetCode::RET_OK;
}


comm::RetCode Producer::MakeAddRequests(const int topic_id,
                                        const std::vector<std::shared_ptr<comm::proto::QItem> > &items,
                                        vector<unique_ptr<comm::proto::AddRequest> > &reqs,
                                        ItemUpdateFunc item_update_func) {

    reqs.clear();

    comm::ProducerBP::GetThreadInstance()->OnMakeAddRequests(topic_id, items);

    comm::RetCode ret;

    shared_ptr<const config::TopicConfig> topic_config;
    if (comm::RetCode::RET_OK != (ret = config::GlobalConfig::GetThreadInstance()->GetTopicConfigByTopicID(topic_id, topic_config))) {
        comm::ProducerBP::GetThreadInstance()->OnValidTopicID(topic_id);
        NLErr("GetTopicConfigByTopicID ret %d", as_integer(ret));
        return ret;
    }

    size_t byte_size_limit = topic_config->GetProto().topic().items_byte_size_limit();
    int batch_limit = topic_config->GetProto().topic().batch_limit();


    map<int, vector<unique_ptr<comm::proto::QItem> > > queue_info_id2new_items;
    for (auto &&item : items) {
        if (nullptr == item) continue;

        auto new_item = unique_ptr<comm::proto::QItem>(new comm::proto::QItem());
        new_item->CopyFrom(*item);
        if (item_update_func) item_update_func(*new_item);

        int queue_info_id = 0;
        if (comm::RetCode::RET_OK != (ret = topic_config->GetQueueInfoIDByCount(new_item->pub_id(), new_item->count(), queue_info_id))) {
            if (comm::RetCode::RET_ERR_RANGE_CNT == ret) {
                comm::ProducerBP::GetThreadInstance()->OnCountLimit(topic_id, new_item->pub_id(), *item);
                NLInfo("skip. GetQueueInfoIDByCount ret %d count %d "
                       "handle_id %d ori_pub_id %d pub_id %d sub_ids %" PRIu64 " hash %" PRIu64 " uin %" PRIu64,
                       as_integer(ret), new_item->count(),
                       new_item->meta().handle_id(), new_item->meta().pub_id(), new_item->pub_id(), (uint64_t)new_item->sub_ids(),
                       (uint64_t)new_item->meta().hash(), (uint64_t)new_item->meta().uin());
            } else {
                NLErr("GetQueueInfoIDByCount ret %d count %d "
                      "handle_id %d ori_pub_id %d pub_id %d sub_ids %" PRIu64 " hash %" PRIu64 " uin %" PRIu64,
                      as_integer(ret), new_item->count(),
                      new_item->meta().handle_id(), new_item->meta().pub_id(), new_item->pub_id(), (uint64_t)new_item->sub_ids(),
                      (uint64_t)new_item->meta().hash(), (uint64_t)new_item->meta().uin());
            }
            continue;
        }

        queue_info_id2new_items[queue_info_id].push_back(move(new_item));
    }


    for (auto &&kv : queue_info_id2new_items) {
        auto &&new_items = kv.second;

        unique_ptr<comm::proto::AddRequest> req = nullptr;
        int batch = 0;
        size_t byte_size = 0;
        for (auto &&new_item : new_items) {
            if (nullptr == new_item) continue;

            auto item_byte_size = new_item->ByteSize();
            if (batch + 1 > batch_limit || byte_size + item_byte_size > byte_size_limit) {
                if (!req || req->items_size() == 0) {
                    comm::ProducerBP::GetThreadInstance()->OnItemSizeTooLarge(topic_id, new_item->pub_id());
                    NLErr("new_item size too large. batch %d batch_limit %d byte_size %zu byte_size_limit %zu item_byte_size %zu",
                          batch, batch_limit,
                          byte_size, byte_size_limit,
                          item_byte_size);
                    return comm::RetCode::RET_ERR_SIZE_TOO_LARGE;
                }
                reqs.push_back(move(req));
                batch = 0;
                byte_size = 0;
            }

            ++batch;
            byte_size += item_byte_size;

            if (!req) {
                req = unique_ptr<comm::proto::AddRequest>(new comm::proto::AddRequest());
                req->set_topic_id(topic_id);
                req->set_store_id(-1);
                req->set_queue_id(-1);
            }


            NLInfo("add item. hash %" PRIu64 " sub_ids %" PRIu64 " pub_id %d store_id %d queue_id %d atime %u count %d",
                   new_item->meta().hash(),
                   new_item->sub_ids(), new_item->pub_id(),
                   req->store_id(), req->queue_id(),
                   new_item->atime(), new_item->count());

            req->add_items()->Swap(new_item.get());

        }

        if (req && req->items_size()) {
            reqs.push_back(move(req));
        }
    }

    comm::ProducerBP::GetThreadInstance()->OnMakeAddRequestsSucc(topic_id, items);

    return comm::RetCode::RET_OK;
}


comm::RetCode Producer::SelectAndAdd(comm::proto::AddRequest &req, StoreSelector *ss, QueueSelector *qs) {
    QLVerb("SelectAndAdd");

    comm::ProducerBP::GetThreadInstance()->OnSelectAndAdd(req);

    if (0 == req.items_size()) return comm::RetCode::RET_OK;

    auto pub_id = req.items(0).pub_id();
    auto uin = req.items(0).meta().uin();
    auto count = req.items(0).count();

    comm::RetCode ret;

    shared_ptr<const config::TopicConfig> topic_config;
    if (comm::RetCode::RET_OK != (ret = config::GlobalConfig::GetThreadInstance()->GetTopicConfigByTopicID(req.topic_id(), topic_config))) {
        comm::ProducerBP::GetThreadInstance()->OnTopicIDInvalid(req);
        QLErr("GetTopicConfigByTopicID ret %d topic_id %d", as_integer(ret), req.topic_id(), uin);
        return ret;
    }

    std::unique_ptr<QueueSelector> default_qs;
    if (!qs) {
        default_qs = NewQueueSelector(req.topic_id(), pub_id, uin, count, topic_config->GetProto().topic().producer_retry_switch_queue());
        qs = default_qs.get();
        comm::ProducerBP::GetThreadInstance()->OnUseDefaultQueueSelector(req);
    } else {
        comm::ProducerBP::GetThreadInstance()->OnUseCustomQueueSelector(req);
    }

    std::unique_ptr<StoreSelector> default_ss;
    if (!ss) {
        default_ss = NewStoreSelector(req.topic_id(), pub_id, uin, topic_config->GetProto().topic().producer_retry_switch_store());
        ss = default_ss.get();
        comm::ProducerBP::GetThreadInstance()->OnUseDefaultStoreSelector(req);
    } else {
        comm::ProducerBP::GetThreadInstance()->OnUseCustomStoreSelector(req);
    }

    int nretry_raw_add{topic_config->GetProto().topic().producer_nretry_raw_add()};
    do {
        int queue_id;
        if (comm::RetCode::RET_OK != (ret = qs->GetQueueID(queue_id))) {
            comm::ProducerBP::GetThreadInstance()->OnGetQueueIDFail(req);
            QLErr("GetQueue ret %d", as_integer(ret));
            return ret;
        }
        req.set_queue_id(queue_id);

        int store_id;
        if (comm::RetCode::RET_OK != (ret = ss->GetStoreID(store_id))) {
            comm::ProducerBP::GetThreadInstance()->OnGetStoreIDFail(req);
            QLErr("GetStore ret %d", as_integer(ret));
            return ret;
        }
        req.set_store_id(store_id);

        if (nullptr != impl_->batch_helper) {
            if (comm::RetCode::RET_OK != (ret = impl_->batch_helper->BatchRawAdd(req))) {
                comm::ProducerBP::GetThreadInstance()->OnBatchRawAddFail(req);
                QLErr("BatchRawEnqueue ret %d store %d queue %d uin %" PRIu64, as_integer(ret), store_id, queue_id, uin);
            }
        } else {
            if (comm::RetCode::RET_OK != (ret = RawAdd(req))) {
                comm::ProducerBP::GetThreadInstance()->OnRawAddFail(req);
                QLErr("RawEnqueue ret %d store %d queue %d uin %" PRIu64, as_integer(ret), store_id, queue_id, uin);
            }
        }

        if (as_integer(ret) >= 0 && comm::RetCode::RET_ERR_NOT_MASTER != ret && comm::RetCode::RET_ERR_NO_MASTER != ret) break;
        --nretry_raw_add;
    } while (nretry_raw_add >= 0);

    if (as_integer(ret) >= 0) comm::ProducerBP::GetThreadInstance()->OnSelectAndAddSucc(req);

    return ret;
}

comm::RetCode Producer::RawAdd(comm::proto::AddRequest &req) {
    QLVerb("RawEnqueue");

    comm::ProducerBP::GetThreadInstance()->OnRawAdd(req);

    if (0 == req.items().size()) return comm::RetCode::RET_OK;

    comm::RetCode ret;

    shared_ptr<const config::TopicConfig> topic_config;
    if (comm::RetCode::RET_OK != (ret = config::GlobalConfig::GetThreadInstance()->GetTopicConfigByTopicID(req.topic_id(), topic_config))) {
        QLErr("GetTopicConfigByTopicID ret %d topic_id %d", as_integer(ret), req.topic_id());
        return ret;
    }

    std::shared_ptr<const config::proto::QueueInfo> queue_info;
    if (comm::RetCode::RET_OK != (ret = topic_config->GetQueueInfoByQueue(req.queue_id(), queue_info))) {
        QLErr("GetQueueInfoByQueue ret %d queue_id %d", as_integer(ret), req.queue_id());
        return ret;
    }
    if (queue_info->drop_all()) return comm::RetCode::RET_OK;

    comm::proto::AddResponse resp;

    BeforeAdd(req);

    store::StoreMasterClient<comm::proto::AddRequest, comm::proto::AddResponse> store_master_client;
    if (comm::RetCode::RET_OK != (ret = store_master_client.ClientCall(req, resp, bind(&Producer::Add, this, placeholders::_1, placeholders::_2)))) {
        comm::ProducerBP::GetThreadInstance()->OnMasterClientCallFail(req);
        QLErr("StoreMasterClient::ClientCall ret %d", as_integer(ret));
    }

    if (comm::RetCode::RET_OK != ret) {
        QLErr("Add ret %d", as_integer(ret));
        return ret;
    }

    AfterAdd(req, resp);

    comm::ProducerBP::GetThreadInstance()->OnRawAddSucc(req);

    QLVerb("RawAdd succ. req: topic_id %d store_id %d queue_id %d items_size %d", req.topic_id(), req.store_id(), req.queue_id(), req.items_size());

    return comm::RetCode::RET_OK;
}

unique_ptr<QueueSelector> Producer::NewQueueSelector(const int topic_id, const int pub_id, const uint64_t uin, const int count, const bool producer_retry_switch_queue) {
    return unique_ptr<QueueSelector>(new QueueSelectorDefault(topic_id, pub_id, uin, count, producer_retry_switch_queue));
}

unique_ptr<StoreSelector> Producer::NewStoreSelector(const int topic_id, const int pub_id, const uint64_t uin, const bool retry_switch_store) {
    return unique_ptr<StoreSelector>(new StoreSelectorDefault(topic_id, pub_id, uin, retry_switch_store));
}


}  // namespace producer

}  // namespace phxqueue

