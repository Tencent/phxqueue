/*
Tencent is pleased to support the open source community by making PhxQueue available.
Copyright (C) 2017 THL A29 Limited, a Tencent company. All rights reserved.
Licensed under the BSD 3-Clause License (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at

<https://opensource.org/licenses/BSD-3-Clause>

Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.
*/



#include "phxqueue/config/schedulerconfig.h"

#include "phxqueue/comm.h"


namespace phxqueue {

namespace config {


using namespace std;


class SchedulerConfig::SchedulerConfigImpl {
  public:
    SchedulerConfigImpl() {}
    virtual ~SchedulerConfigImpl() {}

    shared_ptr<proto::Scheduler> scheduler;
};

SchedulerConfig::SchedulerConfig() : impl_(new SchedulerConfigImpl()){
    assert(impl_);
}

SchedulerConfig::~SchedulerConfig() {
}

comm::RetCode SchedulerConfig::ReadConfig(proto::SchedulerConfig &proto) {
    // sample
    proto.Clear();

    proto::Scheduler *scheduler = nullptr;
    comm::proto::Addr *addr = nullptr;

    // scheduler 1
    {
        scheduler = proto.mutable_scheduler();

        addr = scheduler->add_addrs();
        addr->set_ip("127.0.0.1");
        addr->set_port(6100);
        addr->set_paxos_port(0);

        addr = scheduler->add_addrs();
        addr->set_ip("127.0.0.1");
        addr->set_port(6200);
        addr->set_paxos_port(0);

        addr = scheduler->add_addrs();
        addr->set_ip("127.0.0.1");
        addr->set_port(6300);
        addr->set_paxos_port(0);
    }

    return comm::RetCode::RET_OK;
}


comm::RetCode SchedulerConfig::Rebuild() {
    impl_->scheduler = nullptr;

    auto &&proto = GetProto();
    impl_->scheduler = make_shared<proto::Scheduler>(proto.scheduler());

    return comm::RetCode::RET_OK;
}


comm::RetCode SchedulerConfig::GetScheduler(shared_ptr<const proto::Scheduler> &scheduler) const {
    scheduler = impl_->scheduler;
    return comm::RetCode::RET_OK;
}


}  // namespace config

}  // namespace phxqueue


//gzrd_Lib_CPP_Version_ID--start
#ifndef GZRD_SVN_ATTR
#define GZRD_SVN_ATTR "0"
#endif
static char gzrd_Lib_CPP_Version_ID[] __attribute__((used))="$HeadURL$ $Id$ " GZRD_SVN_ATTR "__file__";
// gzrd_Lib_CPP_Version_ID--end

