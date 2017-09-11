/*
Tencent is pleased to support the open source community by making PhxQueue available.
Copyright (C) 2017 THL A29 Limited, a Tencent company. All rights reserved.
Licensed under the BSD 3-Clause License (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at

<https://opensource.org/licenses/BSD-3-Clause>

Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.
*/



#include "phxqueue/store/basemgr.h"

#include <cinttypes>

#include "phxpaxos/node.h"
#include "phxqueue/comm.h"
#include "phxqueue/config.h"
#include "phxqueue/plugin.h"

#include "phxqueue/store/checkpointstat.h"
#include "phxqueue/store/proto/store.pb.h"
#include "phxqueue/store/storemeta.h"
#include "phxqueue/store/storesm.h"
#include "phxqueue/store/syncctrl.h"


namespace phxqueue {

namespace store {


using namespace std;
using namespace phxpaxos;


class BaseMgr::BaseMgrImpl {
  public:
    BaseMgrImpl() {}
    virtual ~BaseMgrImpl() {}

    Store *store{nullptr};
    std::unique_ptr<StoreMetaQueue[]> meta_queues{nullptr};

};


BaseMgr::BaseMgr(Store *store) : impl_(new BaseMgrImpl()) {
    impl_->store = store;
}

BaseMgr::~BaseMgr() {}


comm::RetCode BaseMgr::Init() {
    //locks_.reset(new Mutex[conf_->GetIDCNum() * conf_->GetQueueNum()]);
    impl_->meta_queues.reset(new StoreMetaQueue[impl_->store->GetStoreOption()->nqueue]);

    //delay_stat_.reset(new DelayStat(topic_id_, conf_->GetQueueNum()));

    return comm::RetCode::RET_OK;
}


comm::RetCode BaseMgr::Add(const uint64_t cursor_id, const comm::proto::AddRequest &req) {
    QLVerb("Add. req: topic_id %d store_id %d queue_id %d", req.topic_id(), req.store_id(), req.queue_id());
    comm::RetCode ret;

    //__OssAttrQueue(req.queue());

    const int topic_id = impl_->store->GetTopicID();

    shared_ptr<const config::TopicConfig> topic_config;
    if (comm::RetCode::RET_OK != (ret = config::GlobalConfig::GetThreadInstance()->GetTopicConfigByTopicID(topic_id, topic_config))) {
        QLErr("GetTopicConfigByTopicID ret %d topic_id %d", as_integer(ret), topic_id);
        return ret;
    }

    comm::StoreBaseMgrBP::GetThreadInstance()->OnAdd(req);

    shared_ptr<const config::proto::QueueInfo> queue_info;
    if (comm::RetCode::RET_OK != (ret = topic_config->GetQueueInfoByQueue(req.queue_id(), queue_info))) {
        QLErr("GetQueueInfoByQueue ret %d", as_integer(ret));
        return ret;
    }

    if (0 == req.items_size()) return comm::RetCode::RET_OK;

    if (NeedSkipAdd(cursor_id, req)) {
        comm::StoreBaseMgrBP::GetThreadInstance()->OnAddSkip(req);
        //if (ossid) OssAttrInc(ossid, phxqueue::ossattr::store::ADD_SKIP, req.items_size());
        return comm::RetCode::RET_ADD_SKIP;
    }

    auto &&meta_queue = impl_->meta_queues[req.queue_id()];
    meta_queue.PushBack(std::move(StoreMeta(cursor_id)));

    comm::StoreBaseMgrBP::GetThreadInstance()->OnAddSucc(req, cursor_id);

    return comm::RetCode::RET_OK;
}

bool BaseMgr::NeedSkipAdd(const uint64_t cursor_id, const comm::proto::AddRequest &req) {
    comm::RetCode ret;

    if (0 == req.items_size()) {
        QLInfo("add req skip. item size 0");
        return true;
    }

    const int topic_id = impl_->store->GetTopicID();

    StoreMetaQueue &meta_queue = impl_->meta_queues[req.queue_id()];
    StoreMeta back_meta;
    if (meta_queue.Back(back_meta) && back_meta.GetCursorID() >= cursor_id) {
        // cursor_id already at meta_queue's back mostly in batch propose sistuation.
        return true;
    }

    shared_ptr<const config::TopicConfig> topic_config;
    if (comm::RetCode::RET_OK != (ret = config::GlobalConfig::GetThreadInstance()->GetTopicConfigByTopicID(topic_id, topic_config))) {
        QLErr("GetTopicConfig ret %d", as_integer(ret));
        return true;
    }

    if (!topic_config->IsValidQueue(req.queue_id())) {
        QLErr("IsValidQueue fail. queue_id %d", req.queue_id());
        return true;
    }
/*
    shared_ptr<const config::proto::Pub> pub;
    if (comm::RetCode::RET_OK != (ret = topic_config->GetPubByPubID(req.pub_id(), pub))) {
        QLErr("GetPubByPubID ret %d pub_id %d", as_integer(ret), req.pub_id());
        return true;
    }

    SyncCtrl *sync = impl_->store->GetSyncCtrl();
    for (size_t i{0}; i < pub->sub_ids_size(); ++i) {
        auto &&sub_id(pub->sub_ids(i));

        uint64_t next_cursor_id;
        ret = sync->GetCursorID(sub_id, req.queue_id(), next_cursor_id, false);
        if (0 < as_integer(ret)) {
            QLWarn("GetCursorID ret %d sub_id %d queue_id %d", ret, sub_id, req.queue_id());
            return false;
        } else if (0 == as_integer(ret) && cursor_id <= next_cursor_id) {
            QLInfo("add req skip. cursor_id %" PRIu64 " next_cursor_id %" PRIu64, cursor_id, next_cursor_id);
            return true;
        }
    }
*/
    return false;
}

comm::RetCode BaseMgr::Get(const comm::proto::GetRequest &req, comm::proto::GetResponse &resp) {
    comm::RetCode ret;

    const int topic_id = impl_->store->GetTopicID();

    shared_ptr<const config::TopicConfig> topic_config;
    if (comm::RetCode::RET_OK != (ret = config::GlobalConfig::GetThreadInstance()->GetTopicConfigByTopicID(topic_id, topic_config))) {
        QLErr("GetTopicConfigByTopicID ret %d topic_id %d", as_integer(ret), topic_id);
        return ret;
    }

    comm::StoreBaseMgrBP::GetThreadInstance()->OnGet(req);

    if (req.queue_id() >= impl_->store->GetStoreOption()->nqueue) {
        QLErr("queue_id %d invalid. nqueue %d", req.queue_id(), impl_->store->GetStoreOption()->nqueue);
        return comm::RetCode::RET_ERR_RANGE_QUEUE;
    }

    QLVerb("begin. req.prev_cursor_id %llu req.next_cursor_id %llu", req.prev_cursor_id(), req.next_cursor_id());

    uint64_t cli_prev_cursor_id{req.prev_cursor_id()};
    uint64_t cli_next_cursor_id{req.next_cursor_id()};

    StoreMetaQueue &meta_queue = impl_->meta_queues[req.queue_id()];

    SyncCtrl *sync = impl_->store->GetSyncCtrl();

    if (as_integer(ret = sync->AdjustNextCursorID(req.sub_id(), req.queue_id(), cli_prev_cursor_id, cli_next_cursor_id)) < 0) {
        comm::StoreBaseMgrBP::GetThreadInstance()->OnAdjustNextCursorIDFail(req);
        QLErr("AdjustCur failed sub_id %d queue_id %d", req.sub_id(), req.queue_id());
        return comm::RetCode::RET_ERR_GET_ADJUST_CURSOR_ID_FAIL;
    } else if (as_integer(ret)) {
        comm::StoreBaseMgrBP::GetThreadInstance()->OnCursorIDNotFound(req);
        QLInfo("cursorid not found. ret %d sub_id %d queue_id %d", as_integer(ret), req.sub_id(), req.queue_id());
        cli_prev_cursor_id = cli_next_cursor_id = -1;
    }

    if (cli_prev_cursor_id != req.prev_cursor_id() || cli_next_cursor_id != req.next_cursor_id()) {
        comm::StoreBaseMgrBP::GetThreadInstance()->OnCursorIDChange(req);
        QLInfo("AdjustCur sub_id %d queue_id %d prev_cursor_id %" PRIu64
               " -> %" PRIu64 " next_cursor_id %" PRIu64 " -> %" PRIu64,
               req.sub_id(), req.queue_id(),
               static_cast<uint64_t>(req.prev_cursor_id()), cli_prev_cursor_id,
               static_cast<uint64_t>(req.next_cursor_id()), cli_next_cursor_id);
    }

    // get cp
    uint64_t cp = -1, min_instance_id = -1;
    do {
        const int paxos_group_id(req.queue_id() % impl_->store->GetStoreOption()->ngroup);
        auto stat = impl_->store->GetCheckPointStatMgr()->GetCheckPointStat(paxos_group_id);
        if (!stat) {
            QLErr("GetCheckPointStat fail paxos_group_id %d", paxos_group_id);
            break;
        }

        uint64_t tmp_cp;
        if (comm::RetCode::RET_OK != (ret = stat->GetCheckPoint(tmp_cp))) {
            QLErr("GetCheckPoint ret %d paxos_group_id %d", ret, paxos_group_id);
            break;
        }
        cp = tmp_cp;
        min_instance_id = impl_->store->GetNode()->GetMinChosenInstanceID(paxos_group_id);
    } while (0);


    uint64_t cur_cursor_id, prev_cursor_id{cli_prev_cursor_id}, next_cursor_id{cli_next_cursor_id};


    size_t byte_size{0};
    for (uint32_t i{0}; 512 > i && resp.items_size() < req.limit(); ++i) {

        cur_cursor_id = next_cursor_id;

        if (-1 == cp || cur_cursor_id >= cp) {
            comm::StoreBaseMgrBP::GetThreadInstance()->OnGetItemFromStoreMetaQueue(req);

            bool crc_chk_pass;
            StoreMeta meta;
            if (!meta_queue.Next(StoreMeta(cur_cursor_id), meta, crc_chk_pass)) {
                comm::StoreBaseMgrBP::GetThreadInstance()->OnGetNoMoreItem(req, cur_cursor_id);
                QLVerb("queue_id %d no more item, skipped. items_size %d",
                       req.queue_id(), resp.items_size());
                break;
            }
            cur_cursor_id = meta.GetCursorID();

            if (crc_chk_pass) comm::StoreBaseMgrBP::GetThreadInstance()->OnCrcCheckPass(req);
            else comm::StoreBaseMgrBP::GetThreadInstance()->OnCrcCheckUnpass(req);

        } else { // replay
            comm::StoreBaseMgrBP::GetThreadInstance()->OnGetItemBeforeCheckPoint(req);

            if (-1 != min_instance_id && cur_cursor_id < min_instance_id)
                cur_cursor_id = min_instance_id;
            else ++cur_cursor_id;
        }


        if (prev_cursor_id != -1 && prev_cursor_id >= cur_cursor_id) {
            QLVerb("skip items, req.sub_id %d, prev_cursor_id %" PRIu64
                   " >= cur_cursor_id %" PRIu64,
                   req.sub_id(), prev_cursor_id, cur_cursor_id);
            next_cursor_id = cur_cursor_id;
            continue;
        }

        vector<comm::proto::QItem> items;
        if (0 > as_integer(ret = GetItemsByCursorID(req.queue_id(), cur_cursor_id, items))) {
            comm::StoreBaseMgrBP::GetThreadInstance()->
                    OnGetItemsByCursorIDFail(req, cur_cursor_id);
            QLErr("GetItemsByCursorID ret %d queue_id %d cur_cursor_id %" PRIu64,
                  as_integer(ret), req.queue_id(), cur_cursor_id);
            return comm::RetCode::RET_ERR_GET_ITEM_BY_CURSOR_ID;
        } else if (as_integer(ret)) {
            comm::StoreBaseMgrBP::GetThreadInstance()->
                    OnGetLastItemNotChosenInPaxos(req, cur_cursor_id);
            QLVerb("GetItemsByCursorID ret %d queue_id %d cur_cursor_id %" PRIu64,
                   ret, req.queue_id(), cur_cursor_id);
            break;
        }

        if (0 == items.size()) {
            next_cursor_id = cur_cursor_id;
            continue;
        }

        // 由于batch_propose的原因, items.size()可能会超过req.limit().
        // 为防止永远get不到数据, 这里必定会返回数据给consumer, 然后由consumer保证每个forward或addretry请求的req.items_size()不超过batch_limit.
        if (resp.items_size() && resp.items_size() + items.size() > req.limit()) {
            QLVerb("resp_item_size %d item_size %zu limit %d",
                   resp.items_size(), items.size(), req.limit());
            break;
        }

        if (req.atime() != 0) {
            if (items[0].atime() > req.atime()) {
                comm::StoreBaseMgrBP::GetThreadInstance()->OnGetNoMoreItemBeforeATime(req);
                QLVerb("queue_id %d no more item before atime %u", req.queue_id(), req.atime());
                break;
            }
        }

        next_cursor_id = cur_cursor_id;


        for (auto &&item : items) {
            if (impl_->store->SkipGet(item, req)) {
                comm::StoreBaseMgrBP::GetThreadInstance()->OnGetSkip(req, item);
                QLVerb("skip item, uin %llu handle_id %d hash %" PRIu64
                       " sub_ids %llu, request sub_id %d cur_cursor_id %" PRIu64,
                       item.meta().uin(), item.meta().handle_id(),
                       item.meta().hash(), item.sub_ids(), req.sub_id(),
                       cur_cursor_id);
                continue;
            }

            QLVerb("add item to resp. cur_cursor_id %" PRIu64
                   " meta.atime %u req.atime %u uin %llu hash %" PRIu64,
                   cur_cursor_id, item.atime(), req.atime(),
                   item.meta().uin(), item.meta().hash());

            byte_size += item.ByteSize();
            resp.add_items()->Swap(&item);
        }


        if (byte_size >= topic_config->GetProto().topic().items_byte_size_limit()) {
            comm::StoreBaseMgrBP::GetThreadInstance()->OnGetRespSizeExceed(req, byte_size);
            QLVerb("queue_id %d size exceed. byte_size %zu items_size %d",
                   req.queue_id(), byte_size, resp.items_size());
            if (resp.items_size() == 1) {
                comm::StoreBaseMgrBP::GetThreadInstance()->OnGetItemTooBig(req, resp.items(0));
                QLInfo("warning, uin %" PRIu64 " handler_id %d size %zu hash %" PRIu64,
                       resp.items(0).meta().uin(), resp.items(0).meta().handle_id(),
                       resp.items(0).buffer().size(), resp.items(0).meta().hash());
            }
            break;
        }
    }

    resp.set_prev_cursor_id(prev_cursor_id);
    resp.set_next_cursor_id(next_cursor_id);
    QLVerb("set_cursor_id %" PRIu64, next_cursor_id);


    if (prev_cursor_id != -1) {
        if (comm::RetCode::RET_OK !=
            (ret = sync->UpdateCursorID(req.sub_id(), req.queue_id(), prev_cursor_id))) {
            comm::StoreBaseMgrBP::GetThreadInstance()->OnUpdateCursorIDFail(req);
            QLErr("__UpdateCursorID ret %d queue_id %d prev_cursor_id %" PRIu64,
                  ret, req.queue_id(), prev_cursor_id);
            return comm::RetCode::RET_ERR_GET_UPDATE_CURSOR_ID_FAIL;
        }
    }

    if (next_cursor_id != -1) {
        if (comm::RetCode::RET_OK !=
            (ret = sync->UpdateCursorID(req.sub_id(), req.queue_id(), next_cursor_id, false))) {
            comm::StoreBaseMgrBP::GetThreadInstance()->OnUpdateCursorIDFail(req);
            QLErr("__UpdateCursorID ret %d queue_id %d next_cursor_id %" PRIu64,
                  ret, req.queue_id(), next_cursor_id);
            return comm::RetCode::RET_ERR_GET_UPDATE_CURSOR_ID_FAIL;
        }
    }

    if (cli_next_cursor_id != next_cursor_id) {
        comm::StoreBaseMgrBP::GetThreadInstance()->OnItemInResp(req);
    }

    QLInfo("Get end. sub_id %u queue_id %d prev_cursor_id %" PRIu64
           " next_cursor_id %" PRIu64 " size %u",
           req.sub_id(), req.queue_id(), prev_cursor_id, next_cursor_id, resp.items_size());

    comm::StoreBaseMgrBP::GetThreadInstance()->OnGetSucc(req, resp);


    return comm::RetCode::RET_OK;
}



comm::RetCode BaseMgr::GetItemsByCursorID(const int queue_id, const uint64_t cursor_id,
                                          vector<comm::proto::QItem> &items) {
    items.clear();

    const int paxos_group_id(queue_id % impl_->store->GetStoreOption()->ngroup);


    std::vector<std::pair<std::string, int>> values;
    int paxos_ret = impl_->store->GetNode()->GetInstanceValue(paxos_group_id, cursor_id, values);
    if (phxpaxos::Paxos_GetInstanceValue_Value_Not_Chosen_Yet == paxos_ret) {
        return comm::RetCode::RET_ERR_PAXOS_NOT_CHOSEN;
    } else if (phxpaxos::Paxos_GetInstanceValue_Value_NotExist == paxos_ret) {
        QLWarn("GetInstanceValue not exist paxos_group_id %d cursor_id %" PRIu64,
               paxos_group_id, cursor_id);
        return comm::RetCode::RET_OK;
    } else if (0 != paxos_ret) {
        QLErr("GetInstanceValue paxos_ret %d paxos_group_id %d cursor_id %" PRIu64,
              paxos_ret, paxos_group_id, cursor_id);
        return comm::RetCode::RET_ERR_PAXOS_GET_INSTANCE_VALUE;
    }

    for (auto &&value : values) {
        QLVerb("value.length() %zu", value.first.length());
        if (value.second != StoreSM::ID) continue;

        proto::StorePaxosArgs args;
        if (!args.ParseFromString(value.first)) {
            QLErr("ParseFromString fail");
            return comm::RetCode::RET_ERR_PAXOS_VALUE_PARSE;
        }

        if (!args.add_req().items_size()) continue;
        if (queue_id != args.add_req().queue_id()) continue;

        for (size_t i{0}; i < args.add_req().items_size(); ++i) {
            items.push_back(args.add_req().items(i));
            items.back().set_cursor_id(cursor_id);
            QLVerb("add item. topic_id %d uin %" PRIu64,
                   args.add_req().items(i).meta().topic_id(),
                   args.add_req().items(i).meta().uin());
            QLVerb("back item. topic_id %d uin %" PRIu64,
                   items.back().meta().topic_id(), items.back().meta().uin());
        }
    }

    return comm::RetCode::RET_OK;
}

StoreMetaQueue *BaseMgr::GetMetaQueue(const int queue_id) {
    if (queue_id >= impl_->store->GetStoreOption()->nqueue) {
        QLErr("queue_id %d invalid. nqueue %d", queue_id, impl_->store->GetStoreOption()->nqueue);
        return nullptr;
    }
    return &impl_->meta_queues[queue_id];
}


}  // namespace store

}  // namespace phxqueue

