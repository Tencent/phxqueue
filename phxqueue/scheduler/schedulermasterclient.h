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

