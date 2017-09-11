/*
Tencent is pleased to support the open source community by making PhxQueue available.
Copyright (C) 2017 THL A29 Limited, a Tencent company. All rights reserved.
Licensed under the BSD 3-Clause License (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at

<https://opensource.org/licenses/BSD-3-Clause>

Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.
*/



#pragma once

#include <memory>

#include "phxqueue/comm.h"
#include "phxqueue/config.h"


namespace phxqueue {

namespace scheduler {


template <typename Req, typename Resp>
class SchedulerMasterClient : public comm::MasterClient<Req, Resp> {
  public:
    SchedulerMasterClient() {}
    virtual ~SchedulerMasterClient() override {}

  protected:
    virtual std::string GetRouteKeyByReq(const Req &req) override;
    virtual comm::RetCode GetCandidateAddrs(const Req &req,
                                            std::vector<comm::proto::Addr> &addrs) override;
};

template <typename Req, typename Resp>
std::string SchedulerMasterClient<Req, Resp>::GetRouteKeyByReq(const Req &req) {
    return comm::GetRouteKey(req.topic_id());
}

template <typename Req, typename Resp>
comm::RetCode SchedulerMasterClient<Req, Resp>::GetCandidateAddrs(const Req &req,
                                                                  std::vector<comm::proto::Addr> &addrs) {
    addrs.clear();

    std::shared_ptr<const config::SchedulerConfig> scheduler_config;
    comm::RetCode ret{config::GlobalConfig::GetThreadInstance()->GetSchedulerConfig(req.topic_id(),
                                                                      scheduler_config)};
    if (comm::RetCode::RET_OK != ret) {
        QLErr("GetSchedulerConfig ret %d topic_id %d", as_integer(ret), req.topic_id());

        return ret;
    }

    std::shared_ptr<const config::proto::Scheduler> scheduler;
    ret = scheduler_config->GetScheduler(scheduler);
    if (comm::RetCode::RET_OK != ret) {
        QLErr("GetScheduler ret %d topic_id %d", as_integer(ret), req.topic_id());

        return ret;
    }

    //std::string addrs_str;
    for (int i{0}; scheduler->addrs_size() > i; ++i) {
        addrs.push_back(scheduler->addrs(i));
        //addrs_str += scheduler->addrs(i).ip() + ";";
    }
    //QLVerb("addrs {%s}", addrs_str.c_str());

    return comm::RetCode::RET_OK;
}


}  // namespace scheduler

}  // namespace phxqueue

