/*
Tencent is pleased to support the open source community by making PhxQueue available.
Copyright (C) 2017 THL A29 Limited, a Tencent company. All rights reserved.
Licensed under the BSD 3-Clause License (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at

<https://opensource.org/licenses/BSD-3-Clause>

Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.
*/



#include "phxqueue/store/keepsyncthread.h"

#include <memory>
#include <thread>

#include "phxqueue/comm.h"
#include "phxqueue/config.h"
#include "phxqueue/plugin.h"
#include "phxpaxos/node.h"

#include "phxqueue/store/proto/store.pb.h"
#include "phxqueue/store/syncctrl.h"
#include "phxqueue/store/storemeta.h"
#include "phxqueue/store/basemgr.h"
#include "phxqueue/store/checkpointstat.h"
#include "phxqueue/store/storesm.h"
#include "phxqueue/store/store.h"


namespace phxqueue {

namespace store {


using namespace std;


class KeepSyncThread::KeepSyncThreadImpl {
  public:
    KeepSyncThreadImpl() {}
    virtual ~KeepSyncThreadImpl() {}

    Store *store{nullptr};
    time_t last_clear_time = 0;
    unique_ptr<time_t[]> next_make_cp_times;
    unique_ptr<time_t[]> next_sync_times;
    time_t last_report_backlog_time = 0;

    unique_ptr<thread> t = nullptr;
    bool stop = true;
};

KeepSyncThread::KeepSyncThread(Store *const store) : impl_(new KeepSyncThreadImpl()){
    impl_->store = store;
}

KeepSyncThread::~KeepSyncThread() {
    Stop();
}

void KeepSyncThread::Run() {
    if (!impl_->t) {
        impl_->stop = false;
        impl_->t = unique_ptr<thread>(new thread(&KeepSyncThread::DoRun, this));
    }
    assert(impl_->t);
}

void KeepSyncThread::Stop() {
    if (impl_->t) {
        impl_->stop = true;
        impl_->t->join();
        impl_->t.release();
    }
}

void KeepSyncThread::DoRun() {
    if (!impl_->store) {
        return;
    }

    const int ngroup = impl_->store->GetStoreOption()->ngroup;
    impl_->next_make_cp_times.reset(new time_t[ngroup]);
    impl_->next_sync_times.reset(new time_t[ngroup]);
    for (int i{0}; i < ngroup; ++i) {
        impl_->next_make_cp_times[i] = 0;
        impl_->next_sync_times[i] = 0;
    }

    while (1) {
        if (impl_->stop) return;

        sleep(1);

        ClearSyncCtrl();

        MakeCheckPoint();

        SyncCursorID();

        Replay();

        ReportBacklog();

        ReportDelayStat();
    }
}

void KeepSyncThread::ClearSyncCtrl() {
    QLVerb("KeepSyncThread::ClearSyncCtrl");

    time_t last_clear_time{config::GlobalConfig::GetThreadInstance()->
            GetLastModTime(impl_->store->GetTopicID())};

    if (last_clear_time != impl_->last_clear_time) {
        impl_->last_clear_time = last_clear_time;
        impl_->store->GetSyncCtrl()->ClearSyncCtrl();
    }
}


void KeepSyncThread::MakeCheckPoint() {
    QLVerb("KeepSyncThread::MakeCheckPoint");

    comm::RetCode ret;

    phxpaxos::Node *node = impl_->store->GetNode();
    SyncCtrl *sync = impl_->store->GetSyncCtrl();

    auto opt = impl_->store->GetStoreOption();

    const int topic_id = impl_->store->GetTopicID();
    const int ngroup = opt->ngroup;
    const int nqueue = opt->nqueue;
    const int nconsumer_group = opt->nconsumer_group;

    shared_ptr<const config::TopicConfig> topic_config;
    if (comm::RetCode::RET_OK !=
        (ret = config::GlobalConfig::GetThreadInstance()->
         GetTopicConfigByTopicID(topic_id, topic_config))) {
        QLErr("GetTopicConfigByTopicID ret %d topic_id %d", ret, topic_id);
        return;
    }

    int store_make_cp_interval_s = topic_config->GetProto().topic().store_make_cp_interval_s();
    if (!store_make_cp_interval_s) store_make_cp_interval_s = 1;


    comm::proto::Addr addr;
    addr.set_ip(opt->ip);
    addr.set_port(opt->port);
    addr.set_paxos_port(opt->paxos_port);

    shared_ptr<const config::StoreConfig> store_config;
    if (comm::RetCode::RET_OK !=
        (ret = config::GlobalConfig::GetThreadInstance()->
         GetStoreConfig(topic_id, store_config))) {
        QLErr("GetStoreConfig ret %d topic_id %d", as_integer(ret), topic_id);
        return;
    }

    int store_id;
    if (comm::RetCode::RET_OK != (ret = store_config->GetStoreIDByAddr(addr, store_id))) {
        QLErr("GetStoreIDByAddr ret %d", as_integer(ret));
        return;
    }

    set<int> pub_ids;
    if (comm::RetCode::RET_OK !=
        (ret = config::utils::GetPubIDsByStoreID(topic_id, store_id, pub_ids))) {
        QLErr("GetPubIDsByStoreID ret %d topic_id %d store_id %d", as_integer(ret), topic_id, store_id);
        return;
    }

    for (int paxos_group_id{0}; paxos_group_id < ngroup; ++paxos_group_id) {
        time_t now = time(nullptr);
        QLVerb("KeepSyncThread::MakeCheckPoint now %" PRIu64 " next %" PRIu64,
               now, impl_->next_make_cp_times[paxos_group_id]);

        if (now < impl_->next_make_cp_times[paxos_group_id]) continue;
        impl_->next_make_cp_times[paxos_group_id] = now + store_make_cp_interval_s +
                comm::utils::OtherUtils::FastRand() % store_make_cp_interval_s;


        uint64_t prev_cursor_id, min_prev_cursor_id;
        uint64_t cp{phxpaxos::NoCheckpoint};
        uint64_t now_instance_id{node->GetNowInstanceID(paxos_group_id)};
        bool need_update_meta_queue;
        bool finish{false};

        for (int queue_id{paxos_group_id}; queue_id < nqueue && !finish; queue_id += ngroup) {
            need_update_meta_queue = true;
            min_prev_cursor_id = -1;
            for (int consumer_group_id{1}; consumer_group_id <= nconsumer_group; ++consumer_group_id) {
                bool valid = false;
                for (auto &&pub_id : pub_ids) {
                    if (topic_config->IsValidQueue(queue_id, pub_id, consumer_group_id)) {
                        valid = true;
                        break;
                    }
                }

                bool skip = topic_config->QueueShouldSkip(queue_id, consumer_group_id);
                
                if (valid && !skip) {
                    if (comm::RetCode::RET_OK == (ret = sync->GetCursorID(consumer_group_id, queue_id, prev_cursor_id))) {
                        if (min_prev_cursor_id == -1 || prev_cursor_id < min_prev_cursor_id) {
                            min_prev_cursor_id = prev_cursor_id;
                        }
                    } else if (0 < as_integer(ret)) {
                        need_update_meta_queue = false;
                    }
                }
            }

            auto meta_queue(impl_->store->GetBaseMgr()->GetMetaQueue(queue_id));

            // clear meta_queue
            if (need_update_meta_queue) {
                if (-1 == min_prev_cursor_id) meta_queue->Clear();
                else meta_queue->EraseFrontTill(StoreMeta(min_prev_cursor_id));
            }

            StoreMeta meta;
            int queue_size = meta_queue->Size();
            if (queue_size > 0 && meta_queue->Front(meta)) {
                QLVerb("paxos_group_id %d queue_id %d queue_size %d front %" PRIu64,
                       paxos_group_id, queue_id, queue_size, meta.GetCursorID());

                uint64_t tmp{meta.GetCursorID() - 1};
                if (-1 == tmp) {
                    cp = -1;
                    finish = true;
                }
                if (-1 == cp || tmp < cp) cp = tmp;
            }

            QLVerb("paxos_group_id %d queue_id %d queue_size %d cp %" PRIu64 " min_prev_cursor_id %" PRIu64
                   " now_instance_id %" PRIu64,
                   paxos_group_id, queue_id, queue_size, cp, min_prev_cursor_id, now_instance_id);
        }

        if (-1 == cp && !finish) {  // all queue_id empty
            cp = now_instance_id - 1;
        }
        QLInfo("paxos_group_id %d cp %" PRIu64 " now_instance_id %" PRIu64,
               paxos_group_id, cp, now_instance_id);

        if (0 > as_integer(ret = DumpCheckPoint(paxos_group_id, cp))) {
            QLWarn("DumpCheckPointToFile ret %d paxos_group_id %d cp %" PRIu64, ret, paxos_group_id, cp);
        }

        usleep(10000);
    }
}

comm::RetCode KeepSyncThread::DumpCheckPoint(const int paxos_group_id, const uint64_t cp) {
    QLVerb("KeepSyncThread::DumpCheckPoint");

    if (-1 == cp) return comm::RetCode::RET_ERR_NO_NEED_DUMP_CHECKPOINT;

    comm::RetCode ret;

    auto stat = impl_->store->GetCheckPointStatMgr()->GetCheckPointStat(paxos_group_id);
    if (!stat) {
        QLErr("GetCheckPointStat fail paxos_group_id %d", paxos_group_id);
        return comm::RetCode::RET_ERR_LOGIC;
    }

    uint64_t tmp_cp;
    if (comm::RetCode::RET_OK != (ret = stat->GetCheckPoint(tmp_cp))) {
        QLErr("GetCheckPoint ret %d paxos_group_id %d", ret, paxos_group_id);
        return ret;
    }

    if (-1 != tmp_cp && tmp_cp > cp) return comm::RetCode::RET_ERR_NO_NEED_DUMP_CHECKPOINT;

    if (comm::RetCode::RET_OK != (ret = stat->UpdateCheckPointAndFlush(cp))) {
        QLErr("UpdateCheckPointAndFlush ret %d paxos_group_id %d cp %" PRIu64,
              ret, paxos_group_id, cp);
        return ret;
    }

    return comm::RetCode::RET_OK;
}

void KeepSyncThread::SyncCursorID() {
    QLVerb("KeepSyncThread::SyncCursorID");

    comm::RetCode ret;

    SyncCtrl *sync{impl_->store->GetSyncCtrl()};
    phxpaxos::Node *node{impl_->store->GetNode()};

    const int topic_id{impl_->store->GetTopicID()};

    auto opt(impl_->store->GetStoreOption());

    const int ngroup{opt->ngroup};
    const int nqueue{opt->nqueue};
    const int nconsumer_group{opt->nconsumer_group};

    shared_ptr<const config::TopicConfig> topic_config;
    if (comm::RetCode::RET_OK !=
        (ret = config::GlobalConfig::GetThreadInstance()->
         GetTopicConfigByTopicID(topic_id, topic_config))) {
        QLErr("GetTopicConfigByTopicID ret %d topic_id %d", ret, topic_id);
        return;
    }

    int store_sync_interval_s = topic_config->GetProto().topic().store_sync_interval_s();
    if (!store_sync_interval_s) store_sync_interval_s = 1;


    for (int paxos_group_id{0}; paxos_group_id < ngroup; ++paxos_group_id) {
        if (!node->IsIMMaster(paxos_group_id)) continue;

        time_t now{time(nullptr)};
        if (now < impl_->next_sync_times[paxos_group_id]) continue;
        impl_->next_sync_times[paxos_group_id] = now + store_sync_interval_s +
                comm::utils::OtherUtils::FastRand() % store_sync_interval_s;

        proto::SyncCtrlInfo sync_ctrl_info;
        uint64_t prev_cursor_id;
        for (int queue_id{paxos_group_id}; queue_id < nqueue; queue_id += ngroup) {
            proto::SyncCtrlInfo::QueueDetail queue_detail;
            queue_detail.set_queue_id(queue_id);
            for (int consumer_group_id{1}; consumer_group_id <= nconsumer_group; ++consumer_group_id) {
                if (0 > as_integer(ret = sync->GetCursorID(consumer_group_id, queue_id, prev_cursor_id))) {
                    QLErr("sync->GetCursorID ret %d consumer_group_id %d queue %d",
                          as_integer(ret), consumer_group_id, queue_id);
                } else if (!as_integer(ret)) {
                    proto::SyncCtrlInfo::QueueDetail::ConsumerGroupDetail *consumer_group_detail =
                            queue_detail.add_consumer_group_details();
                    consumer_group_detail->set_consumer_group_id(consumer_group_id);
                    consumer_group_detail->set_prev_cursor_id(prev_cursor_id);
                }
            }
            if (queue_detail.consumer_group_details_size()) {
                sync_ctrl_info.add_queue_details()->CopyFrom(queue_detail);
            }
        }

        // 1. serialize paxos value
        proto::StorePaxosArgs args;
        args.set_timestamp(time(nullptr));
        args.mutable_sync_ctrl_info()->CopyFrom(sync_ctrl_info);

        string buf;
        args.SerializeToString(&buf);

        // 2. send to paxos
        StoreContext sc;
        sc.result = comm::RetCode::RET_OK;

        phxpaxos::SMCtx ctx(StoreSM::ID, &sc);
        uint64_t instance_id;
        int paxos_ret{node->Propose(paxos_group_id, buf, instance_id, &ctx)};
        if (0 != paxos_ret) {
            QLErr("Propose err. paxos_ret %d paxos_group_id %d buf.length %zu",
                  paxos_ret, paxos_group_id, buf.length());
            continue;
        }

        usleep(10000);
    }
}

bool KeepSyncThread::QueueNeedReplay(const consumer::Queue_t &queue, const set<int> &pub_ids,
        const vector<unique_ptr<config::proto::ReplayInfo>> &replay_infos, int &replay_infos_idx) {
    comm::RetCode ret;

    const int topic_id = impl_->store->GetTopicID();
    shared_ptr<const config::TopicConfig> topic_config;
    if (comm::RetCode::RET_OK !=
        (ret = config::GlobalConfig::GetThreadInstance()->
         GetTopicConfigByTopicID(topic_id, topic_config))) {
        QLErr("GetTopicConfigByTopicID ret %d topic_id %d", ret, topic_id);
        return false;
    }

    replay_infos_idx = -1;
    for (int idx{0}; idx < replay_infos.size(); ++idx) {
        auto &&replay_info = replay_infos[idx];
        if (!replay_info) continue;
        if (replay_info->consumer_group_ids_size()) {
            bool found = false;
            for (int i{0}; i < replay_info->consumer_group_ids_size(); ++i) {
                auto consumer_group_id = replay_info->consumer_group_ids(i);
                if (consumer_group_id == queue.consumer_group_id) {
                    found = true;
                    break;
                }
            }
            if (!found) continue;
        }
        if (replay_info->pub_ids_size()) {
            bool found = false;
            for (int i{0}; i < replay_info->pub_ids_size(); ++i) {
                auto pub_id = replay_info->pub_ids(i);
                if (pub_ids.end() != pub_ids.find(pub_id) && topic_config->IsValidQueue(queue.queue_id, pub_id)) {
                    found = true;
                    break;
                }
            }
            if (!found) continue;
        }
        replay_infos_idx = idx;
        return true;
    }
    return false;
}

void KeepSyncThread::Replay() {
    comm::RetCode ret;
    const int topic_id{impl_->store->GetTopicID()};
    auto opt(impl_->store->GetStoreOption());

    shared_ptr<const config::TopicConfig> topic_config;
    if (comm::RetCode::RET_OK !=
        (ret = config::GlobalConfig::GetThreadInstance()->
         GetTopicConfigByTopicID(topic_id, topic_config))) {
        QLErr("GetTopicConfigByTopicID ret %d topic_id %d", ret, topic_id);
        return;
    }

    comm::proto::Addr addr;
    addr.set_ip(opt->ip);
    addr.set_port(opt->port);
    addr.set_paxos_port(opt->paxos_port);

    shared_ptr<const config::StoreConfig> store_config;
    if (comm::RetCode::RET_OK !=
        (ret = config::GlobalConfig::GetThreadInstance()->
         GetStoreConfig(topic_id, store_config))) {
        QLErr("GetStoreConfig ret %d topic_id %d", as_integer(ret), topic_id);
        return;
    }

    int store_id;
    if (comm::RetCode::RET_OK != (ret = store_config->GetStoreIDByAddr(addr, store_id))) {
        QLErr("GetStoreIDByAddr ret %d", as_integer(ret));
        return;
    }

    set<int> pub_ids;
    if (comm::RetCode::RET_OK !=
        (ret = config::utils::GetPubIDsByStoreID(topic_id, store_id, pub_ids))) {
        QLErr("GetPubIDsByStoreID ret %d topic_id %d store_id %d",
              as_integer(ret), topic_id, store_id);
        return;
    }

    vector<unique_ptr<config::proto::ReplayInfo>> replay_infos;
    if (comm::RetCode::RET_OK != (ret = topic_config->GetAllReplayInfo(replay_infos))) {
        QLErr("GetAllReplayInfo ret %d", as_integer(ret));
        return;
    }

    if (!replay_infos.size()) return;

    QLInfo("Do Replay!!!");

    vector<consumer::Queue_t> queues;
    GetAllLocalQueue(queues);

    for (auto &&queue : queues) {
        int replay_infos_idx{-1};
        bool replay = QueueNeedReplay(queue, pub_ids, replay_infos, replay_infos_idx);

        if (replay && -1 != replay_infos_idx) {  // need replay
            auto &&replay_info = replay_infos[replay_infos_idx].get();

            uint64_t min_instance_id{phxpaxos::NoCheckpoint};
            uint64_t now_instance_id{phxpaxos::NoCheckpoint};
            uint64_t replay_instance_id{phxpaxos::NoCheckpoint};
            const int paxos_group_id{queue.queue_id % impl_->store->GetStoreOption()->ngroup};
            min_instance_id = impl_->store->GetNode()->GetMinChosenInstanceID(paxos_group_id);
            now_instance_id = impl_->store->GetNode()->GetNowInstanceID(paxos_group_id);

            QLInfo("need replay. queue_id %d store_id %d consumer_group_id %d ( min_instance_id %"
                   PRIu64 " <= now_instance_id %" PRIu64 " )",
                   queue.queue_id, queue.store_id, queue.consumer_group_id,
                   min_instance_id, now_instance_id);

            if (-1 == now_instance_id) continue;  // nothing to replay

            QLInfo("replay_info cursor_id %" PRIu64 " timestamp %" PRIu64,
                   (uint64_t)replay_info->cursor_id(), (uint64_t)replay_info->timestamp());

            // decide replay_instance_id
            replay_instance_id = ((-1 == min_instance_id) ? 0 : min_instance_id);
            if (-1 != replay_info->cursor_id()) {
                if (replay_info->cursor_id() > now_instance_id) continue;
                if (replay_info->cursor_id() > replay_instance_id)
                    replay_instance_id = replay_info->cursor_id();
            } else {  // cal replay_instance_id by timestamp
                uint64_t l = replay_instance_id;
                uint64_t r = now_instance_id;
                uint64_t mid = -1;
                while (l < r) {
                    mid = l + ((r + 1 - l) >> 1);

                    std::vector<std::pair<std::string, int> > values;
                    int paxos_ret{impl_->store->GetNode()->
                            GetInstanceValue(paxos_group_id, mid, values)};
                    if (phxpaxos::Paxos_GetInstanceValue_Value_Not_Chosen_Yet == paxos_ret) {
                        r = mid - 1;
                        continue;
                    } else if (phxpaxos::Paxos_GetInstanceValue_Value_NotExist == paxos_ret) {
                        QLWarn("GetInstanceValue not exist paxos_group_id %d cursor_id %" PRIu64,
                               paxos_group_id, mid);
                        l = mid;
                        continue;
                    } else if (0 != paxos_ret) {
                        QLErr("GetInstanceValue paxos_ret %d paxos_group_id %d cursor_id %" PRIu64,
                              paxos_ret, paxos_group_id, mid);
                        r = mid - 1;
                        continue;
                    }

                    bool found{false};
                    for (auto &&value : values) {  // just find the first StorePaxosArgs
                        if (value.second != StoreSM::ID) continue;

                        proto::StorePaxosArgs args;
                        if (!args.ParseFromString(value.first)) {
                            QLErr("ParseFromString fail");
                            continue;
                        }

                        found = true;
                        if (args.timestamp() < replay_info->timestamp()) {
                            l = mid;
                            break;
                        } else {
                            r = mid - 1;
                            break;
                        }
                    }
                    if (!found) {
                        r = mid - 1;
                    }
                }
                if (-1 != mid) {
                    replay_instance_id = mid;
                }
            }
            uint64_t ori_prev_cursor_id;
            if (comm::RetCode::RET_OK !=
                (ret = impl_->store->GetSyncCtrl()->
                 GetCursorID(queue.consumer_group_id, queue.queue_id, ori_prev_cursor_id)) ||
                -1 == ori_prev_cursor_id) {
                if (0 > as_integer(ret))
                    QLErr("GetCursorID prev ret %d consumer_group_id %d queue_id %d",
                          as_integer(ret), queue.consumer_group_id, queue.queue_id);
                continue;
            }
            QLInfo("do replay. queue_id %d store_id %d consumer_group_id %d ( cursor_id %" PRIu64
                   " -> %" PRIu64 " )", queue.queue_id, queue.store_id,
                   queue.consumer_group_id, ori_prev_cursor_id, replay_instance_id);
            if (comm::RetCode::RET_OK !=
                (ret = impl_->store->GetSyncCtrl()->
                 UpdateCursorID(queue.consumer_group_id, queue.queue_id, replay_instance_id))) {
                QLErr("UpdateCursorID prev ret %d consumer_group_id %d queue_id %d cursor_id %" PRIu64,
                      as_integer(ret), queue.consumer_group_id, queue.queue_id, replay_instance_id);
            }
            if (comm::RetCode::RET_OK !=
                (ret = impl_->store->GetSyncCtrl()->
                 UpdateCursorID(queue.consumer_group_id, queue.queue_id, replay_instance_id, false))) {
                QLErr("UpdateCursorID next ret %d consumer_group_id %d queue_id %d cursor_id %" PRIu64,
                      as_integer(ret), queue.consumer_group_id, queue.queue_id, replay_instance_id);
            }
        }
    }
}


void KeepSyncThread::ReportBacklog() {
    QLVerb("KeepSyncThread::ReportBacklog");

    comm::RetCode ret;

    vector<consumer::Queue_t> queues;

    GetAllLocalQueue(queues);

    const int topic_id = impl_->store->GetTopicID();
    auto opt = impl_->store->GetStoreOption();
    auto sync = impl_->store->GetSyncCtrl();

    uint64_t cursor_id;
    for (auto &&queue : queues) {
        int paxos_group_id = queue.queue_id % opt->ngroup;
        int immaster = impl_->store->GetNode()->IsIMMaster(paxos_group_id);

        if (as_integer(comm::RetCode::RET_OK) >
            as_integer(ret = sync->GetCursorID(queue.consumer_group_id, queue.queue_id, cursor_id))) {
            QLErr("GetCursorID ret %d consumer_group_id %d queue_id %d", ret, queue.consumer_group_id, queue.queue_id);
            continue;
        }

        int backlog = 0;
        if (comm::RetCode::RET_OK !=
            (ret = sync->GetBackLogByCursorID(queue.queue_id, cursor_id, backlog))) {
            QLErr("GetBackLogByCursorID ret %d", as_integer(ret));
            continue;
        }

        QLInfo("BACKLOG: store_id %d queue_id %d consumer_group_id %d backlog %d cursor_id %"
               PRIu64 " immaster %d",
               queue.store_id, queue.queue_id, queue.consumer_group_id, backlog, cursor_id, immaster);
        if (immaster)
            comm::StoreBacklogBP::GetThreadInstance()->
                    OnBackLogReport(topic_id, queue.consumer_group_id, queue.queue_id, backlog);
    }
}

void KeepSyncThread::ReportDelayStat() {
    QLVerb("KeepSyncThread::ReportDelayStat");
}

void KeepSyncThread::GetAllLocalQueue(vector<consumer::Queue_t> &queues) {
    queues.clear();

    comm::RetCode ret;

    const int topic_id = impl_->store->GetTopicID();

    shared_ptr<const config::TopicConfig> topic_config;
    if (comm::RetCode::RET_OK !=
        (ret = config::GlobalConfig::GetThreadInstance()->
         GetTopicConfigByTopicID(topic_id, topic_config))) {
        QLErr("GetTopicConfigByTopicID ret %d", as_integer(ret));
        return;
    }

    auto opt = impl_->store->GetStoreOption();
    comm::proto::Addr addr;
    addr.set_ip(opt->ip);
    addr.set_port(opt->port);
    addr.set_paxos_port(opt->paxos_port);

    shared_ptr<const config::StoreConfig> store_config;
    if (comm::RetCode::RET_OK !=
        (ret = config::GlobalConfig::GetThreadInstance()->
         GetStoreConfig(topic_id, store_config))) {
        QLErr("GetStoreConfig ret %d topic_id %d", as_integer(ret), topic_id);
        return;
    }

    int store_id;
    if (comm::RetCode::RET_OK != (ret = store_config->GetStoreIDByAddr(addr, store_id))) {
        QLErr("GetStoreIDByAddr ret %d", as_integer(ret));
        return;
    }

    set<int> pub_ids;
    if (comm::RetCode::RET_OK !=
        (ret = config::utils::GetPubIDsByStoreID(topic_id, store_id, pub_ids))) {
        QLErr("GetPubIDsByStoreID ret %d topic_id %d store_id %d",
              as_integer(ret), topic_id, store_id);
        return;
    }

    for (int queue_id{0}; queue_id < opt->nqueue; ++queue_id) {
        for (int consumer_group_id{1}; consumer_group_id <= opt->nconsumer_group; ++consumer_group_id) {
            bool is_valid = false;
            for (auto &&pub_id : pub_ids) {
                if (topic_config->IsValidQueue(queue_id, pub_id, consumer_group_id)) {
                    is_valid = true;
                    break;
                }
            }
            if (!is_valid) continue;

            if (topic_config->QueueShouldSkip(queue_id, consumer_group_id)) continue;

            {
                consumer::Queue_t queue;
                queue.consumer_group_id = consumer_group_id;
                queue.store_id = store_id;
                queue.queue_id = queue_id;

                queues.push_back(queue);
            }
        }
    }
}


}  // namespace store

}  // namespace phxqueue


//gzrd_Lib_CPP_Version_ID--start
#ifndef GZRD_SVN_ATTR
#define GZRD_SVN_ATTR "0"
#endif
static char gzrd_Lib_CPP_Version_ID[] __attribute__((used))="$HeadURL$ $Id$ " GZRD_SVN_ATTR "__file__";
// gzrd_Lib_CPP_Version_ID--end

