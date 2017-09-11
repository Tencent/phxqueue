/*
Tencent is pleased to support the open source community by making PhxQueue available.
Copyright (C) 2017 THL A29 Limited, a Tencent company. All rights reserved.
Licensed under the BSD 3-Clause License (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at

<https://opensource.org/licenses/BSD-3-Clause>

Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.
*/



#include "phxqueue/scheduler/scheduler.h"

#include <mutex>

#include "phxqueue/comm.h"
#include "phxqueue/lock.h"

#include "phxqueue/scheduler/loadbalancethread.h"
#include "phxqueue/scheduler/keepmasterthread.h"
#include "phxqueue/scheduler/schedulermgr.h"


namespace phxqueue {

namespace scheduler {


using namespace std;


class Scheduler::SchedulerImpl {
  public:
    SchedulerImpl() = default;
    virtual ~SchedulerImpl() = default;

    SchedulerOption opt;
    int topic_id{-1};
    unique_ptr<SchedulerMgr> scheduler_mgr;
    unique_ptr<LoadBalanceThread> load_balance_thread;
    unique_ptr<KeepMasterThread> keep_master_thread;

    comm::proto::Addr addr;
};


Scheduler::Scheduler(const SchedulerOption &opt) : impl_(new SchedulerImpl()) {
    assert(impl_);
    impl_->opt = opt;
}

Scheduler::~Scheduler() {
}

comm::RetCode Scheduler::Init() {
    comm::RetCode ret;

    if (impl_->opt.log_func) {
        comm::Logger::GetInstance()->SetLogFunc(impl_->opt.log_func);
    }

    if (impl_->opt.config_factory_create_func) {
        plugin::ConfigFactory::SetConfigFactoryCreateFunc(impl_->opt.config_factory_create_func);
    }

    if (impl_->opt.break_point_factory_create_func) {
        plugin::BreakPointFactory::SetBreakPointFactoryCreateFunc(impl_->opt.break_point_factory_create_func);
    }

    impl_->addr.set_ip(impl_->opt.ip);
    impl_->addr.set_port(impl_->opt.port);

    ret = InitTopicID();
    if (comm::RetCode::RET_OK != ret) {
        QLErr("InitTopicID ret %d", as_integer(ret));

        return ret;
    }

    impl_->scheduler_mgr = unique_ptr<SchedulerMgr>(new SchedulerMgr(this));

    impl_->load_balance_thread = unique_ptr<LoadBalanceThread>(new LoadBalanceThread(this));
    impl_->keep_master_thread = unique_ptr<KeepMasterThread>(new KeepMasterThread(this));

    ret = impl_->scheduler_mgr->Init();
    if (comm::RetCode::RET_OK != ret) {
        QLErr("scheduler_mgr Init ret %d", as_integer(ret));

        return ret;
    }

    impl_->load_balance_thread->Run();
    impl_->keep_master_thread->Run();

    return comm::RetCode::RET_OK;
}

comm::RetCode Scheduler::Dispose() {
    comm::SchedulerBP::GetThreadInstance()->OnDispose();

    if (impl_->keep_master_thread)
        impl_->keep_master_thread->Stop();
    if (impl_->load_balance_thread)
        impl_->load_balance_thread->Stop();

    return impl_->scheduler_mgr->Dispose();
}

const SchedulerOption *Scheduler::GetSchedulerOption() const {
    return &impl_->opt;
}

const comm::proto::Addr &Scheduler::GetAddr() {
    return impl_->addr;
}

SchedulerMgr *Scheduler::GetSchedulerMgr() {
    return impl_->scheduler_mgr.get();
}

comm::RetCode Scheduler::InitTopicID() {
    comm::RetCode ret;

    if (!impl_->opt.topic.empty()) {
        if (comm::RetCode::RET_OK != (ret = config::GlobalConfig::GetThreadInstance()->GetTopicIDByTopicName(impl_->opt.topic, impl_->topic_id))) {
            QLErr("GetTopicIDByTopicName ret %d topic %s", as_integer(ret), impl_->opt.topic.c_str());
        }
        return ret;
    }

    QLInfo("no topic name. find toipc_id by addr(%s:%d)", impl_->addr.ip().c_str(), impl_->addr.port());

    set<int> topic_ids;
    if (comm::RetCode::RET_OK != (ret = config::GlobalConfig::GetThreadInstance()->GetAllTopicID(topic_ids))) {
        QLErr("GetAllTopicID ret %d", as_integer(ret));
        return ret;
    }

    for (auto &&topic_id : topic_ids) {

        QLVerb("check topic_id %d", topic_id);

        shared_ptr<const config::SchedulerConfig> scheduler_config;
        if (comm::RetCode::RET_OK != (ret = config::GlobalConfig::GetThreadInstance()->GetSchedulerConfig(topic_id, scheduler_config))) {
            QLErr("GetSchedulerConfig ret %d", as_integer(ret));
            continue;
        }

        shared_ptr<const config::proto::Scheduler> scheduler;
        if (comm::RetCode::RET_OK != (ret = scheduler_config->GetScheduler(scheduler))) {
            QLErr("GetScheduler ret %d", as_integer(ret));
        } else {
            for (int i{0}; i < scheduler->addrs_size(); ++i) {
                auto &&addr = scheduler->addrs(i);
                if (addr.ip() == impl_->addr.ip() && addr.port() == impl_->addr.port()) {
                    impl_->topic_id = topic_id;
                    QLInfo("found toipc_id %d addr (%s:%d)", topic_id, impl_->addr.ip().c_str(), impl_->addr.port());
                    return comm::RetCode::RET_OK;
                }
            }
        }
    }

    return comm::RetCode::RET_ERR_RANGE_ADDR;
}

int Scheduler::GetTopicID() {
    return impl_->topic_id;
}

comm::RetCode Scheduler::GetAddrScale(const comm::proto::GetAddrScaleRequest &req,
                                      comm::proto::GetAddrScaleResponse &resp) {
    comm::SchedulerBP::GetThreadInstance()->OnGetAddrScale(req);

    comm::RetCode ret;
    ret = impl_->scheduler_mgr->GetAddrScale(req, resp);
    if (0 > as_integer(ret)) {
        QLErr("GetAddrScale ret %d", as_integer(ret));
        comm::SchedulerBP::GetThreadInstance()->OnGetAddrScaleFail(req);
        return ret;
    }

    comm::SchedulerBP::GetThreadInstance()->OnGetAddrScaleSucc(req, resp);
    return ret;
}

comm::RetCode Scheduler::RawGetLockInfo(comm::proto::GetLockInfoRequest &req,
                                        comm::proto::GetLockInfoResponse &resp) {
    lock::LockMasterClient<comm::proto::GetLockInfoRequest, comm::proto::GetLockInfoResponse>
            lock_master_client;
    comm::RetCode ret{lock_master_client.ClientCall(req, resp,
            bind(&Scheduler::GetLockInfo, this, placeholders::_1, placeholders::_2))};
    if (comm::RetCode::RET_OK != ret) {
        comm::SchedulerBP::GetThreadInstance()->OnGetLockInfoFail(req);
        QLErr("SchedulerMasterClient::ClientCall ret %d", as_integer(ret));
    }

    if (comm::RetCode::RET_OK != ret) {
        QLErr("Add ret %d", as_integer(ret));
        return ret;
    }

    comm::SchedulerBP::GetThreadInstance()->OnGetLockInfoSucc(req, resp);

    QLVerb("RawGetLockInfo succ");

    return comm::RetCode::RET_OK;
}

comm::RetCode Scheduler::RawAcquireLock(comm::proto::AcquireLockRequest &req,
                                        comm::proto::AcquireLockResponse &resp) {
    lock::LockMasterClient<comm::proto::AcquireLockRequest, comm::proto::AcquireLockResponse>
            lock_master_client;
    comm::RetCode ret{lock_master_client.ClientCall(req, resp,
            bind(&Scheduler::AcquireLock, this, placeholders::_1, placeholders::_2))};
    if (comm::RetCode::RET_OK != ret) {
        comm::SchedulerBP::GetThreadInstance()->OnAcquireLockFail(req);
        QLErr("SchedulerMasterClient::ClientCall ret %d", as_integer(ret));
    }

    if (comm::RetCode::RET_OK != ret) {
        QLErr("Add ret %d", as_integer(ret));
        return ret;
    }

    comm::SchedulerBP::GetThreadInstance()->OnAcquireLockSucc(req, resp);

    QLVerb("RawAcquireLock succ");

    return comm::RetCode::RET_OK;
}

bool Scheduler::NeedDropMaster() {
    return false;
}

void Scheduler::OnKeepMasterThreadRun() {
}

bool Scheduler::NeedSkipUpdateLoad(const comm::proto::GetAddrScaleRequest &req) {
    return false;
}


}  // namespace scheduler

}  // namespace phxqueue

