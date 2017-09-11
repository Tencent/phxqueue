/*
Tencent is pleased to support the open source community by making PhxQueue available.
Copyright (C) 2017 THL A29 Limited, a Tencent company. All rights reserved.
Licensed under the BSD 3-Clause License (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at

<https://opensource.org/licenses/BSD-3-Clause>

Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.
*/



#include "phxqueue/scheduler/loadbalancethread.h"

#include <thread>
#include <unistd.h>

#include "phxqueue/comm.h"
#include "phxqueue/config.h"

#include "phxqueue/scheduler/schedulermgr.h"


namespace phxqueue {

namespace scheduler {


using namespace std;


class LoadBalanceThread::LoadBalanceThreadImpl {
  public:
    LoadBalanceThreadImpl() {}
    virtual ~LoadBalanceThreadImpl() {}

    Scheduler *scheduler{nullptr};

    unique_ptr<thread> t{nullptr};
    bool stop{true};
};


LoadBalanceThread::LoadBalanceThread(Scheduler *const scheduler)
        : impl_(new LoadBalanceThreadImpl()) {
    impl_->scheduler = scheduler;
}

LoadBalanceThread::~LoadBalanceThread() {
    Stop();
}

void LoadBalanceThread::Run() {
    if (!impl_->t) {
        impl_->stop = false;
        impl_->t = unique_ptr<thread>(new thread(&LoadBalanceThread::DoRun, this));
    }
    assert(impl_->t);
}

void LoadBalanceThread::Stop() {
    if (impl_->t) {
        impl_->stop = true;
        impl_->t->join();
        impl_->t.release();
    }
}

void LoadBalanceThread::DoRun() {
    comm::RetCode ret;
    const int topic_id{impl_->scheduler->GetTopicID()};

    while (true) {

        shared_ptr<const config::TopicConfig> topic_config;
        if (comm::RetCode::RET_OK != (ret = config::GlobalConfig::GetThreadInstance()->GetTopicConfigByTopicID(topic_id, topic_config))) {
            QLErr("GetTopicConfigByTopicID ret %d", as_integer(ret));
            sleep(1);
            continue;
        }

        const uint64_t now{comm::utils::Time::GetSteadyClockMS()};

        if (!impl_->scheduler->GetSchedulerMgr()->IsMaster(now)) {
            QLInfo("not master");
            sleep(topic_config->GetProto().topic().scheduler_load_balance_interval_s());

            continue;
        }

        ret = impl_->scheduler->GetSchedulerMgr()->LoadBalance(now);
        if (comm::RetCode::RET_OK != ret) {
            QLErr("LoadBalance err %d", ret);
        }

        sleep(topic_config->GetProto().topic().scheduler_load_balance_interval_s());
    }
}


}  // namespace scheduler

}  // namespace phxqueue

