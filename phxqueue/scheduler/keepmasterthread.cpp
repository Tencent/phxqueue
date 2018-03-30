/*
Tencent is pleased to support the open source community by making PhxQueue available.
Copyright (C) 2017 THL A29 Limited, a Tencent company. All rights reserved.
Licensed under the BSD 3-Clause License (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at

<https://opensource.org/licenses/BSD-3-Clause>

Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.
*/



#include "phxqueue/scheduler/keepmasterthread.h"

#include <thread>
#include <unistd.h>

#include "phxqueue/comm.h"
#include "phxqueue/config.h"

#include "phxqueue/scheduler/schedulermgr.h"


namespace phxqueue {

namespace scheduler {


using namespace std;


class KeepMasterThread::KeepMasterThreadImpl {
  public:
    KeepMasterThreadImpl() {}
    virtual ~KeepMasterThreadImpl() {}

    Scheduler *scheduler{nullptr};
    int master_rate{0};

    unique_ptr<thread> t;
    bool stop{true};
};


KeepMasterThread::KeepMasterThread(Scheduler *const scheduler)
        : impl_(new KeepMasterThreadImpl()) {
    impl_->scheduler = scheduler;
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
    impl_->scheduler->OnKeepMasterThreadRun();

    comm::RetCode ret{InitMasterRate()};
    if (comm::RetCode::RET_OK != ret) {
        QLErr("MASTERSTAT: InitMasterRate ret %d", ret);

        exit(-1);
    }

    while (true) {
        if (comm::RetCode::RET_OK != (ret = AdjustMasterRate())) {
            QLErr("MASTERSTAT: AdjustMasterRate ret %d", ret);
        }
        if (comm::RetCode::RET_OK != (ret = KeepMaster())) {
            QLErr("MASTERSTAT: KeepMaster ret %d", ret);
        }

        QLInfo("sleep 10");
        sleep(10);
    }
}

comm::RetCode KeepMasterThread::InitMasterRate() {
    impl_->master_rate = 100;

    QLInfo("MASTERSTAT: master_rate %d", impl_->master_rate);

    return comm::RetCode::RET_OK;
}

comm::RetCode KeepMasterThread::AdjustMasterRate() {
    // TODO:
    //MMPHXQueueSchedConfig *conf{mgr_->mutable_conf()};

    //Comm::ZKMgrClient *ptZKMgrClient{Comm::ZKMgrClient::GetDefault()};
    //if (ptZKMgrClient && ptZKMgrClient->IsSvrBlocked(conf->GetEpollConfig()->GetSvrIP(),
    //                                                 conf->GetEpollConfig()->GetSvrPort())) {
    //    OssAttr4SvrClientMasterHostShieldGlobal(conf->GetEpollConfig()->GetOssAttrID(), 1);
    //    QLInfo("svr %s:%d blocked", conf->GetEpollConfig()->GetSvrIP(), conf->GetEpollConfig()->GetSvrPort());
    //    impl_->master_rate = 0;
    //} else {
    //    impl_->master_rate = 100;
    //}

    QLInfo("MASTERSTAT: master_rate %d", impl_->master_rate);

    return comm::RetCode::RET_OK;
}

comm::RetCode KeepMasterThread::KeepMaster() {
    const int topic_id{impl_->scheduler->GetTopicID()};

    comm::SchedulerKeepMasterBP::GetThreadInstance()->OnKeepMaster(topic_id);

    if (impl_->scheduler->NeedDropMaster()) {
        QLInfo("MASTERSTAT: NeedDropMaster return true");

        return comm::RetCode::RET_ERR_SVR_BLOCK;
    }

    comm::RetCode ret;

    string client_id{impl_->scheduler->GetSchedulerOption()->ip};

    int lock_id{-1};
    if (comm::RetCode::RET_OK != (ret = GetLockID(lock_id))) {
        QLErr("GetLockID ret %d", as_integer(ret));

        return ret;
    }

    shared_ptr<const config::TopicConfig> topic_config;
    if (comm::RetCode::RET_OK != (ret = config::GlobalConfig::GetThreadInstance()->GetTopicConfigByTopicID(topic_id, topic_config))) {
        QLErr("GetTopicConfigByTopicID ret %d", as_integer(ret));

        return ret;
    }

    const string lock_key(topic_config->GetProto().topic().scheduler_lock_key());

    static thread_local uint64_t version{0};
    static thread_local uint64_t overdue_time_ms{0};
    uint64_t now{comm::utils::Time::GetSteadyClockMS()};
    bool need_acquire_lock{false};
    bool is_master_origin{impl_->scheduler->GetSchedulerMgr()->IsMaster(now)};
    {
        comm::proto::GetLockInfoRequest req;
        comm::proto::GetLockInfoResponse resp;
        req.set_topic_id(topic_id);
        req.set_lock_id(lock_id);
        req.set_lock_key(lock_key);
        ret = impl_->scheduler->RawGetLockInfo(req, resp);
        if (comm::RetCode::RET_ERR_KEY_NOT_EXIST != ret && comm::RetCode::RET_OK != ret) {
            QLErr("MASTERSTAT: RawGetLockInfo err %d", as_integer(ret));
            comm::SchedulerKeepMasterBP::GetThreadInstance()->OnRawGetLockInfoFail(topic_id, req);

            return ret;
        }
        comm::SchedulerKeepMasterBP::GetThreadInstance()->OnRawGetLockInfoSucc(topic_id, req, resp);

        QLInfo("MASTERSTAT: RawGetLockInfo ret %d lock_key %s old_version %" PRIu64 " old_overdue_time_ms %" PRIu64, as_integer(ret), lock_key.c_str(), version, overdue_time_ms);
        if (comm::RetCode::RET_ERR_KEY_NOT_EXIST == ret) {
            version = 0;
            overdue_time_ms = now;
        } else if (version != resp.lock_info().version()) {
            version = resp.lock_info().version();
            overdue_time_ms = now + resp.lock_info().lease_time_ms();
        }
        QLInfo("MASTERSTAT: new_version %" PRIu64 " new_overdue_time_ms %" PRIu64, version, overdue_time_ms);

        if (comm::RetCode::RET_ERR_KEY_NOT_EXIST == ret || client_id == resp.lock_info().client_id() || now > overdue_time_ms)
            need_acquire_lock = true;
    }

    if (need_acquire_lock) {
        comm::proto::AcquireLockRequest req;
        comm::proto::AcquireLockResponse resp;
        req.set_topic_id(topic_id);
        req.set_lock_id(lock_id);
        auto &&lock_info = req.mutable_lock_info();
        lock_info->set_lock_key(lock_key);
        lock_info->set_version(version);
        lock_info->set_client_id(client_id);
        lock_info->set_lease_time_ms(topic_config->GetProto().topic().scheduler_lock_lease_time_s() * 1000ULL);
        uint64_t expire_time_ms{now + lock_info->lease_time_ms()};

        comm::SchedulerKeepMasterBP::GetThreadInstance()->OnAcquireLock(topic_id, req);
        ret = impl_->scheduler->RawAcquireLock(req, resp);
        if (comm::RetCode::RET_OK == ret) {
            if (!is_master_origin) impl_->scheduler->GetSchedulerMgr()->OnBecomeNewMaster(now);

            ret = impl_->scheduler->GetSchedulerMgr()->RenewMaster(expire_time_ms);
            QLVerb("MASTERSTAT: renew master %llu", lock_info->lease_time_ms());
            comm::SchedulerKeepMasterBP::GetThreadInstance()->OnAcquireLockSucc(topic_id, req, resp);
        } else {
            QLErr("MASTERSTAT: RawAcquireLock ret %d", as_integer(ret));
            comm::SchedulerKeepMasterBP::GetThreadInstance()->OnAcquireLockFail(topic_id, req);
        }

        return ret;
    }

    return comm::RetCode::RET_NO_NEED_LOCK;
}

comm::RetCode KeepMasterThread::GetLockID(int &lock_id) {
    const int topic_id{impl_->scheduler->GetTopicID()};

    comm::RetCode ret;

    shared_ptr<const config::LockConfig> lock_config;
    if (comm::RetCode::RET_OK != (ret = config::GlobalConfig::GetThreadInstance()->GetLockConfig(topic_id, lock_config))) {
        QLErr("GetLockConfig ret %d", as_integer(ret));
        return ret;
    }

    set<int> lock_ids;
    if (comm::RetCode::RET_OK != (ret = lock_config->GetAllLockID(lock_ids))) {
        QLErr("GetAllLockID ret %d", as_integer(ret));
        return ret;
    }

    if (lock_ids.empty()) {
        QLErr("GetAllLockID lock_ids empty");
        return comm::RetCode::RET_ERR_RANGE_LOCK;

    }

    lock_id = *lock_ids.begin();
    return comm::RetCode::RET_OK;
}


}  // namespace scheduler

}  // namespace phxqueue

