/*
Tencent is pleased to support the open source community by making PhxQueue available.
Copyright (C) 2017 THL A29 Limited, a Tencent company. All rights reserved.
Licensed under the BSD 3-Clause License (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at

<https://opensource.org/licenses/BSD-3-Clause>

Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.
*/



#include "phxqueue/store/keepmasterthread.h"

#include <cinttypes>
#include <thread>

#include "phxqueue/comm.h"
#include "phxqueue/config.h"
#include "phxqueue/plugin.h"
#include "phxpaxos/node.h"

#include "phxqueue/store/proto/store.pb.h"
#include "phxqueue/store/store.h"
#include "phxqueue/store/storesm.h"
#include "phxqueue/store/basemgr.h"
#include "phxqueue/store/storemeta.h"


namespace phxqueue {

namespace store {


using namespace std;


class KeepMasterThread::KeepMasterThreadImpl {
  public:
    KeepMasterThreadImpl() {}
    virtual ~KeepMasterThreadImpl() {}

    Store *store{nullptr};
    int master_rate{0};
    time_t last_update_paxos_args_time{0};

    unique_ptr<thread> t;
    bool stop{true};
};


KeepMasterThread::KeepMasterThread(Store *const store) : impl_(new KeepMasterThreadImpl()) {
    impl_->store = store;
}

KeepMasterThread::~KeepMasterThread() {
    Stop();
}

void KeepMasterThread::Run() {
    if (!impl_->t) {
        impl_->stop = false;
        impl_->t = unique_ptr<thread>(new thread(&KeepMasterThread::DoRun, this));
    }
    assert(impl_->t);
}

void KeepMasterThread::Stop() {
    if (impl_->t) {
        impl_->stop = true;
        impl_->t->join();
        impl_->t.release();
    }
}


void KeepMasterThread::DoRun() {
    comm::RetCode ret;

    const int topic_id = impl_->store->GetTopicID();

    InitMasterRate();

    shared_ptr<const config::TopicConfig> topic_config;
    if (comm::RetCode::RET_OK !=
        (ret = config::GlobalConfig::GetThreadInstance()->
         GetTopicConfigByTopicID(topic_id, topic_config))) {
        QLErr("GetTopicConfigByTopicID ret %d topic_id %d", ret, topic_id);
        exit(-1);
    }

    while (true) {
        if (impl_->stop) return;

        sleep(topic_config->GetProto().topic().store_adjust_master_rate_time_interval_s());

        UpdatePaxosArgs();

        AdjustMasterRate();

        KeepMaster();
    }
}

void KeepMasterThread::InitMasterRate() {
    comm::RetCode ret;

    const int topic_id{impl_->store->GetTopicID()};

    impl_->master_rate = 0;

    auto opt(impl_->store->GetStoreOption());

    phxpaxos::Node *node{impl_->store->GetNode()};

    shared_ptr<const config::TopicConfig> topic_config;
    if (comm::RetCode::RET_OK !=
        (ret = config::GlobalConfig::GetThreadInstance()->
         GetTopicConfigByTopicID(topic_id, topic_config))) {
        QLErr("GetTopicConfigByTopicID ret %d topic_id %d", ret, topic_id);
        return;
    }

    for (int paxos_group_id{0}; paxos_group_id < opt->ngroup; ++paxos_group_id) {
        if (node->IsIMMaster(paxos_group_id)) {
            impl_->master_rate = paxos_group_id * 100 / opt->ngroup + (paxos_group_id * 100 % opt->ngroup ? 1 : 0);
        }
    }

    if (impl_->master_rate < topic_config->GetProto().topic().store_adjust_min_master_rate()) {
        impl_->master_rate = topic_config->GetProto().topic().store_adjust_min_master_rate();
    } else if (impl_->master_rate > topic_config->GetProto().topic().store_adjust_max_master_rate()) {
        impl_->master_rate = topic_config->GetProto().topic().store_adjust_max_master_rate();
    }
}

void KeepMasterThread::AdjustMasterRate() {
    comm::RetCode ret;

    const int topic_id{impl_->store->GetTopicID()};

    shared_ptr<const config::TopicConfig> topic_config;
    if (comm::RetCode::RET_OK !=
        (ret = config::GlobalConfig::GetThreadInstance()->
         GetTopicConfigByTopicID(topic_id, topic_config))) {
        QLErr("GetTopicConfigByTopicID ret %d topic_id %d", ret, topic_id);
        return;
    }

    impl_->master_rate = topic_config->GetProto().topic().store_adjust_max_master_rate();
}

void KeepMasterThread::KeepMaster() {
    comm::RetCode ret;

    const int topic_id(impl_->store->GetTopicID());

    shared_ptr<const config::TopicConfig> topic_config;
    if (comm::RetCode::RET_OK !=
        (ret = config::GlobalConfig::GetThreadInstance()->
         GetTopicConfigByTopicID(topic_id, topic_config))) {
        QLErr("GetTopicConfigByTopicID ret %d topic_id %d", ret, topic_id);
        return;
    }

    auto opt(impl_->store->GetStoreOption());

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

    shared_ptr<const config::proto::Store> store;
    if (comm::RetCode::RET_OK != (ret = store_config->GetStoreByAddr(addr, store))) {
        QLErr("GetStoreByAddr ret %d", as_integer(ret));
        return;
    }

    int idx_in_store{-1};
    for (size_t i{0}; i < store->addrs_size(); ++i) {
        if (addr.ip() == store->addrs(i).ip() &&
            addr.port() == store->addrs(i).port() &&
            addr.paxos_port() == store->addrs(i).paxos_port()) {
            idx_in_store = i;
            break;
        }
    }

    auto node(impl_->store->GetNode());

    if (-1 == idx_in_store) return;

    for (int paxos_group_id{0}; paxos_group_id < opt->ngroup; ++paxos_group_id) {
        if (static_cast<double>(paxos_group_id) / opt->ngroup * 100.0 + 0.5 > impl_->master_rate) {
            QLInfo("MASTERSTAT: master_rate too low, need drop master. "
                   "paxos_group_id %d master_rate %d", paxos_group_id, impl_->master_rate);
            node->DropMaster(paxos_group_id);
            continue;
        }

        if (impl_->store->NeedDropMaster(paxos_group_id)) {
            QLInfo("MASTERSTAT: NeedDropMaster return true. paxos_group_id %d", paxos_group_id);
            node->DropMaster(paxos_group_id);
            continue;
        }

        if (node->IsIMMaster(paxos_group_id)) {
            comm::StoreIMMasterBP::GetThreadInstance()->OnIMMaster(topic_id, paxos_group_id);
            QLInfo("MASTERSTAT: i am master. paxos_group_id %d master_rate %d",
                   paxos_group_id, impl_->master_rate);
            continue;
        }

        if (idx_in_store != paxos_group_id % store->addrs_size()) continue;

        QLInfo("MASTERSTAT: begin try to get back master. paxos_group_id %d master_rate %d",
               paxos_group_id, impl_->master_rate);

        // 1. serialize paxos value
        proto::StorePaxosArgs args;
        args.set_timestamp(time(nullptr));
        args.mutable_master_addr()->CopyFrom(addr);

        string buf;
        args.SerializeToString(&buf);

        // 2. send to paxos
        StoreContext sc;
        sc.result = comm::RetCode::RET_OK;

        phxpaxos::SMCtx ctx(StoreSM::ID, &sc);
        uint64_t instance_id;
        comm::StoreSnatchMasterBP::GetThreadInstance()->OnSnatchMaster(topic_id, paxos_group_id);
        int paxos_ret{node->Propose(paxos_group_id, buf, instance_id, &ctx)};
        if (0 != paxos_ret) {
            QLErr("MASTERSTAT: ERR: Propose err. paxos_ret %d paxos_group_id %d buf.length %zu",
                  paxos_ret, paxos_group_id, buf.length());
            continue;
        }

        if (comm::RetCode::RET_OK != sc.result) {
            QLErr("MASTERSTAT: ERR: paxos_group_id %d sc.result %d instance_id %" PRIu64,
                  paxos_group_id, as_integer(sc.result), instance_id);
            continue;
        }

        QLInfo("MASTERSTAT: INFO: Propose ok. ret %d paxos_group_id %d instance_id %" PRIu64,
               as_integer(sc.result), paxos_group_id, instance_id);
    }
}

void KeepMasterThread::UpdatePaxosArgs() {
    comm::RetCode ret;

    const int topic_id{impl_->store->GetTopicID()};

    shared_ptr<const config::TopicConfig> topic_config;
    if (comm::RetCode::RET_OK !=
        (ret = config::GlobalConfig::GetThreadInstance()->
         GetTopicConfigByTopicID(topic_id, topic_config))) {
        QLErr("GetTopicConfigByTopicID ret %d topic_id %d", ret, topic_id);
        return;
    }

    if (impl_->last_update_paxos_args_time == topic_config->GetLastModTime()) {
        return;
    }

    auto opt = impl_->store->GetStoreOption();
    auto nr_group = opt->ngroup;

    auto node = impl_->store->GetNode();

    node->SetTimeoutMs(topic_config->GetProto().topic().store_paxos_propose_timeout_ms());
    node->SetHoldPaxosLogCount(topic_config->GetProto().topic().store_paxos_hold_log_count());
    for (int paxos_group_id{0}; paxos_group_id < nr_group; ++paxos_group_id) {
        node->SetMaxHoldThreads(paxos_group_id,
                topic_config->GetProto().topic().store_paxos_max_hold_threads());
        node->SetMasterLease(paxos_group_id,
                topic_config->GetProto().topic().store_paxos_master_lease());
        node->SetProposeWaitTimeThresholdMS(paxos_group_id,
                topic_config->GetProto().topic().store_paxos_propose_wait_time_threshold_ms());
        node->SetLogSync(paxos_group_id,
                topic_config->GetProto().topic().store_paxos_log_sync());
        node->SetBatchCount(paxos_group_id,
                topic_config->GetProto().topic().store_paxos_batch_count());
        node->SetBatchDelayTimeMs(paxos_group_id,
                topic_config->GetProto().topic().store_paxos_batch_delay_time_ms());
    }

    impl_->last_update_paxos_args_time = topic_config->GetLastModTime();

    QLInfo("paxos args updated");
}


}  // namespace store

}  // namespace phxqueue


//gzrd_Lib_CPP_Version_ID--start
#ifndef GZRD_SVN_ATTR
#define GZRD_SVN_ATTR "0"
#endif
static char gzrd_Lib_CPP_Version_ID[] __attribute__((used))="$HeadURL$ $Id$ " GZRD_SVN_ATTR "__file__";
// gzrd_Lib_CPP_Version_ID--end

