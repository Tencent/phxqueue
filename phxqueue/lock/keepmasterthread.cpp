/*
Tencent is pleased to support the open source community by making PhxQueue available.
Copyright (C) 2017 THL A29 Limited, a Tencent company. All rights reserved.
Licensed under the BSD 3-Clause License (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at

<https://opensource.org/licenses/BSD-3-Clause>

Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.
*/



#include "phxqueue/lock/keepmasterthread.h"

#include <cinttypes>
#include <memory>
#include <thread>
#include <unistd.h>

#include "phxpaxos/node.h"

#include "phxqueue/comm.h"
#include "phxqueue/config.h"
#include "phxqueue/plugin.h"

#include "phxqueue/lock/lock.h"
#include "phxqueue/lock/lockdb.h"
#include "phxqueue/lock/locksm.h"


namespace phxqueue {

namespace lock {


using namespace std;


class KeepMasterThread::KeepMasterThreadImpl {
  public:
    KeepMasterThreadImpl() {}
    virtual ~KeepMasterThreadImpl() {}

    Lock *lock{nullptr};
    int master_rate{0};
    time_t last_update_paxos_args_time{0};

    unique_ptr<thread> t;
    bool stop{true};
};


KeepMasterThread::KeepMasterThread(Lock *const lock) : impl_(new KeepMasterThreadImpl()) {
    impl_->lock = lock;
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
    if (nullptr == impl_->lock || nullptr == impl_->lock->GetNode()) {
        QLErr("lock %p", impl_->lock);

        return;
    }

    comm::RetCode ret{InitMasterRate()};
    if (comm::RetCode::RET_OK != ret) {
        QLErr("InitMasterRate ret %d", ret);

        exit(-1);
    }

    const int topic_id{impl_->lock->GetTopicID()};
    shared_ptr<const config::TopicConfig> topic_config;
    ret = config::GlobalConfig::GetThreadInstance()->GetTopicConfigByTopicID(topic_id, topic_config);
    if (comm::RetCode::RET_OK != ret) {
        QLErr("ERR: GetTopicConfigByTopicID ret %d topic_id %d", ret, topic_id);
        exit(-1);
    }

    while (true) {
        if (impl_->stop) return;

        sleep(topic_config->GetProto().topic().lock_adjust_master_rate_time_interval_s());

        if (comm::RetCode::RET_OK != (ret = UpdatePaxosArgs())) {
            QLErr("UpdatePaxosArgs ret %d", ret);
        }

        if (comm::RetCode::RET_OK != (ret = AdjustMasterRate())) {
            QLErr("AdjustMasterRate ret %d", ret);
        }
        if (comm::RetCode::RET_OK != (ret = KeepMaster())) {
            QLErr("KeepMaster ret %d", ret);
        }
    }
}

comm::RetCode KeepMasterThread::InitMasterRate() {
    impl_->master_rate = 100;

    QLInfo("masterrate %d", impl_->master_rate);

    return comm::RetCode::RET_OK;
}

comm::RetCode KeepMasterThread::AdjustMasterRate() {
    const int topic_id{impl_->lock->GetTopicID()};

    shared_ptr<const config::TopicConfig> topic_config;
    comm::RetCode ret{config::GlobalConfig::GetThreadInstance()->GetTopicConfigByTopicID(topic_id, topic_config)};
    if (comm::RetCode::RET_OK != ret) {
        QLErr("GetTopicConfigByTopicID ret %d topic %d", ret, topic_id);

        return ret;
    }

    impl_->master_rate = topic_config->GetProto().topic().lock_adjust_max_master_rate();

    // TODO:
    //MMPHXLockConfig *conf{mgr_->mutable_conf()};

    //Comm::ZKMgrClient *ptZKMgrClient{Comm::ZKMgrClient::GetDefault()};
    //if (ptZKMgrClient && ptZKMgrClient->IsSvrBlocked(conf->GetEpollConfig()->GetSvrIP(),
    //                                                 conf->GetEpollConfig()->GetSvrPort())) {
    //    OssAttr4SvrClientMasterHostShieldGlobal(conf->GetEpollConfig()->GetOssAttrID(), 1);
    //    QLInfo("svr %s:%d blocked", conf->GetEpollConfig()->GetSvrIP(), conf->GetEpollConfig()->GetSvrPort());
    //    impl_->master_rate = 0;
    //} else {
    //    impl_->master_rate = 100;
    //}

    //QLInfo("masterrate %d", impl_->master_rate);

    return comm::RetCode::RET_OK;
}

comm::RetCode KeepMasterThread::KeepMaster() {
    const int topic_id{impl_->lock->GetTopicID()};

    shared_ptr<const config::TopicConfig> topic_config;
    comm::RetCode ret{config::GlobalConfig::GetThreadInstance()->
            GetTopicConfigByTopicID(topic_id, topic_config)};
    if (comm::RetCode::RET_OK != ret) {
        QLErr("GetTopicConfigByTopicID ret %d topic_id %d", ret, topic_id);

        return ret;
    }

    static vector<bool> last_master;
    const auto &opt(impl_->lock->GetLockOption());
    last_master.resize(opt->nr_group);

    comm::proto::Addr addr;
    addr.set_ip(opt->ip);
    addr.set_port(opt->port);
    addr.set_paxos_port(opt->paxos_port);

    shared_ptr<const config::LockConfig> lock_config;
    ret = config::GlobalConfig::GetThreadInstance()->GetLockConfig(topic_id, lock_config);
    if (comm::RetCode::RET_OK != ret) {
        QLErr("GetLockConfig ret %d topic_id %d", as_integer(ret), topic_id);

        return ret;
    }

    shared_ptr<const config::proto::Lock> lock;
    ret = lock_config->GetLockByAddr(addr, lock);
    if (comm::RetCode::RET_OK != ret) {
        QLErr("GetLockByAddr ret %d", as_integer(ret));

        return ret;
    }

    int idx_in_lock{-1};
    for (size_t i{0}; lock->addrs_size() > i; ++i) {
        if (addr.ip() == lock->addrs(i).ip() &&
            addr.port() == lock->addrs(i).port() &&
            addr.paxos_port() == lock->addrs(i).paxos_port()) {
            idx_in_lock = i;

            break;
        }
    }

    auto node(impl_->lock->GetNode());

    if (-1 == idx_in_lock)
        return comm::RetCode::RET_ERR_LOGIC;

    for (int paxos_group_id{0}; paxos_group_id < opt->nr_group; ++paxos_group_id) {
        if (static_cast<double>(paxos_group_id) / opt->nr_group * 100.0 + 0.5 >
            impl_->master_rate) {
            QLInfo("MASTERSTAT: master_rate too low, need drop master. "
                   "paxos_group_id %d master_rate %d", paxos_group_id, impl_->master_rate);
            node->DropMaster(paxos_group_id);

            continue;
        }

        if (impl_->lock->NeedDropMaster(paxos_group_id)) {
            QLInfo("MASTERSTAT: NeedDropMaster return true. paxos_group_id %d", paxos_group_id);
            node->DropMaster(paxos_group_id);

            continue;
        }

        if (node->IsIMMaster(paxos_group_id)) {
            comm::LockIMMasterBP::GetThreadInstance()->OnIMMaster(topic_id, paxos_group_id);
            QLInfo("MASTERSTAT: i am master. paxos_group_id %d master_rate %d",
                   paxos_group_id, impl_->master_rate);

            continue;
        }

        if (idx_in_lock != paxos_group_id % lock->addrs_size())
            continue;

        QLInfo("MASTERSTAT: begin try to get back master. paxos_group_id %d master_rate %d",
               paxos_group_id, impl_->master_rate);

        // 1. serialize paxos value
        proto::LockPaxosArgs args;
        args.mutable_master_addr()->CopyFrom(addr);

        string buf;
        args.SerializeToString(&buf);

        // 2. send to paxos
        LockContext sc;
        sc.result = comm::RetCode::RET_OK;

        phxpaxos::SMCtx ctx(LockSM::ID, &sc);
        uint64_t instance_id;
        comm::LockSnatchMasterBP::GetThreadInstance()->OnSnatchMaster(topic_id, paxos_group_id);
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

    // TODO:
    //string master_paxos_group_ids;
    //string win_master_paxos_group_ids;
    //string lost_master_paxos_group_ids;
    //string drop_master_paxos_group_ids;
    //for (int paxos_group_id{0}; mgr_->conf()->GetGroupNum() > paxos_group_id;
    //     ++paxos_group_id) {
    //    if (!node_->IsIMMaster(paxos_group_id)) {
    //        if (last_master.at(paxos_group_id)) {
    //            lost_master_paxos_group_ids += to_string(paxos_group_id) + " ";
    //            OssAttrInc(mgr_->oss_attr_id(), 4u, 1u);
    //        }
    //        last_master[paxos_group_id] = false;
    //        OssAttrInc(mgr_->oss_attr_id(), 2u, 1u);
    //    } else {
    //        master_paxos_group_ids += to_string(paxos_group_id) + " ";
    //        if (!last_master.at(paxos_group_id)) {
    //            win_master_paxos_group_ids += to_string(paxos_group_id) + " ";
    //            OssAttrInc(mgr_->oss_attr_id(), 3u, 1u);
    //        }
    //        last_master[paxos_group_id] = true;
    //        OssAttrInc(mgr_->oss_attr_id(), 1u, 1u);
    //    }

    //    // drop the back part of groups that i hold, part size is depend on master rate
    //    if (static_cast<double>(paxos_group_id) /
    //        mgr_->conf()->GetGroupNum() * 100.0 + 0.5 > impl_->master_rate) {
    //        node_->DropMaster(paxos_group_id);
    //        drop_master_paxos_group_ids += to_string(paxos_group_id) + " ";

    //        continue;
    //    }

    //    if (!node_->IsIMMaster(paxos_group_id)) {
    //        if (idx_in_group != paxos_group_id % group_size)
    //            continue;

    //        ProposeMaster(paxos_group_id, addr);
    //    }

    //}  // foreach group

    //QLInfo("master {%s} win {%s} lost {%s} drop {%s} masterrate %d",
    //       master_paxos_group_ids.c_str(), win_master_paxos_group_ids.c_str(),
    //       lost_master_paxos_group_ids.c_str(),
    //       drop_master_paxos_group_ids.c_str(), impl_->master_rate);

    return comm::RetCode::RET_OK;
}

//comm::RetCode KeepMasterThread::GetIdxInGroupAndGroupSize(int &idx_in_group, int &group_size) {
//    idx_in_group = -1;
//    group_size = -1;
//
//    int ret{comm::RetCode::RET_ERR_UNINIT};
//
//    PHXLogic::PHXPaxosInitHelper::PaxosAddr addr;
//    if (0 != (ret = GetLocalAddr(addr))) {
//        QLErr("GetLocalAddr ret %d", ret);
//
//        return comm::RetCode::RET_ERR_LOGIC;
//    }
//
//    int topic_id{-1};
//    int group_id{-1};
//    const string local_host{LOGID_BASE::GetInnerIP()};
//    const MMPHXLockConfig *conf{mgr_->conf()};
//    ret = MMPHXLockGlobalConfig::GetDefault()->
//        GetTopicIDAndGroupIDByPaxosAddr(local_host, conf->GetPaxosPort(), &topic_id, &group_id);
//    if (comm::RetCode::RET_OK != ret) {
//        QLErr("GetTopicIDAndGroupIDByPaxosAddr ret %d localhost %s paxosport %d",
//                ret, local_host.c_str(), conf->GetPaxosPort());
//
//        return comm::RetCode::RET_ERR_LOGIC;
//    }
//
//    const MMPHXLockGroupConfig *group_conf{nullptr};
//    ret = MMPHXLockGlobalConfig::GetDefault()->GetLocalIDCGroupConfigByTopicID(topic_id, group_conf);
//    if (comm::RetCode::RET_OK != ret) {
//        QLErr("GetLocalIDCGroupConfigByTopicID ret %d tid %d", ret, topic_id);
//
//        return comm::RetCode::RET_ERR_LOGIC;
//    }
//    if (!group_conf) {
//        QLErr("GetLocalIDCGroupConfigByTopicID gconf null tid %d", topic_id);
//
//        return comm::RetCode::RET_ERR_RANGE_TOPIC;
//    }
//
//    const Group *group{nullptr};
//    ret = group_conf->GetGroupByGroupID(group_id, group);
//    if (0 != ret) {
//        QLErr("GetGroupByGroupID ret %d tid %d gid %d", ret, topic_id, group_id);
//
//        return comm::RetCode::RET_ERR_LOGIC;
//    }
//    if (!group) {
//        QLErr("GetGroupByGroupID err g null tid %d gid %d", topic_id, group_id);
//
//        return comm::RetCode::RET_ERR_RANGE_GROUP;
//    }
//
//    string host;
//    int port{-1}, paxos_port{-1};
//    group_size = group->addrs_size();
//    for (int i{0}; group_size > i; ++i) {
//        if (!ParseAddr(group->addrs(i), &host, &port, &paxos_port)) {
//            QLErr("ParseAddr err addr %s tid %d gid %d",
//                  group->addrs(i).c_str(), topic_id, group_id);
//
//            return comm::RetCode::RET_ERR_LOGIC;
//        }
//        if (PHXLogic::PHXPaxosInitHelper::GenPaxosAddr(host, paxos_port) == addr) {
//            idx_in_group = i;
//
//            return comm::RetCode::RET_OK;
//        }
//    }
//
//    return comm::RetCode::RET_ERR_RANGE;
//}
//
//comm::RetCode KeepMasterThread::GetLocalAddr(PHXLogic::PHXPaxosInitHelper::PaxosAddr &addr) {
//    const MMPHXLockConfig *conf{mgr_->conf()};
//    if (!conf) return -1;
//    addr = move(PHXLogic::PHXPaxosInitHelper::GenPaxosAddr(LOGID_BASE::GetInnerIP(),
//                                                           conf->GetPaxosPort()));
//
//    return comm::RetCode::RET_OK;
//}

comm::RetCode KeepMasterThread::ProposeMaster(const int paxos_group_id,
                                              const comm::proto::Addr addr) {
    QLInfo("paxos_group_id %d try be master", paxos_group_id);

    // 1. serialize paxos value
    proto::LockPaxosArgs args;
    args.mutable_master_addr()->CopyFrom(addr);

    string buf;
    args.SerializeToString(&buf);

    // 2. send to paxos
    LockContext lc;
    phxpaxos::SMCtx sm_ctx(LockSM::ID, &lc);
    uint64_t instance_id{0};
    int paxos_ret{impl_->lock->GetNode()->Propose(paxos_group_id, buf, instance_id, &sm_ctx)};
    const int topic_id{impl_->lock->GetTopicID()};
    if (phxpaxos::PaxosTryCommitRet_OK != paxos_ret) {
        QLErr("paxos_group_id %d Propose err %d buf.size %zu", paxos_group_id, paxos_ret, buf.size());
        comm::LockKeepMasterThreadBP::GetThreadInstance()->
                OnProposeMasterSucc(topic_id, paxos_group_id);
    } else {
        QLInfo("paxos_group_id %d Propose ok instance_id %" PRIu64 " buf.size %zu",
               paxos_group_id, instance_id, buf.size());
        comm::LockKeepMasterThreadBP::GetThreadInstance()->
                OnProposeMasterErr(topic_id, paxos_group_id);
    }

    return comm::RetCode::RET_OK;
}

comm::RetCode KeepMasterThread::UpdatePaxosArgs() {
    const int topic_id{impl_->lock->GetTopicID()};

    shared_ptr<const config::TopicConfig> topic_config;
    comm::RetCode ret{config::GlobalConfig::GetThreadInstance()->
            GetTopicConfigByTopicID(topic_id, topic_config)};
    if (comm::RetCode::RET_OK != ret) {
        QLErr("GetTopicConfigByTopicID ret %d topic_id %d", ret, topic_id);

        return ret;
    }

    if (impl_->last_update_paxos_args_time == topic_config->GetLastModTime()) {
        return comm::RetCode::RET_OK;
    }

    auto opt(impl_->lock->GetLockOption());
    auto nr_group(opt->nr_group);

    auto node(impl_->lock->GetNode());

    node->SetTimeoutMs(topic_config->GetProto().topic().lock_paxos_propose_timeout_ms());
    node->SetHoldPaxosLogCount(topic_config->GetProto().topic().lock_paxos_nr_hold_log());
    for (int paxos_group_id{0}; paxos_group_id < nr_group; ++paxos_group_id) {
        node->SetMaxHoldThreads(paxos_group_id, topic_config->GetProto().topic().lock_paxos_max_hold_threads());
        node->SetMasterLease(paxos_group_id, topic_config->GetProto().topic().lock_paxos_master_lease());
        node->SetProposeWaitTimeThresholdMS(paxos_group_id, topic_config->GetProto().topic().lock_paxos_propose_wait_time_threshold_ms());
        node->SetLogSync(paxos_group_id, topic_config->GetProto().topic().lock_paxos_log_sync());
        //node->SetBatchCount(paxos_group_id, topic_config->GetProto().topic().lock_paxos_batch_count());
        node->SetBatchDelayTimeMs(paxos_group_id, topic_config->GetProto().topic().lock_paxos_batch_delay_time_ms());
    }

    impl_->last_update_paxos_args_time = topic_config->GetLastModTime();

    QLInfo("paxos args updated");

    return comm::RetCode::RET_OK;
}


}  // namespace lock

}  // namespace phxqueue

