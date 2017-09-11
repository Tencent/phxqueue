#pragma once

#include "phxqueue/comm.h"
#include "phxqueue/scheduler.h"


namespace phxqueue_phxrpc {

namespace scheduler {


class Scheduler : public phxqueue::scheduler::Scheduler {
  public:
    Scheduler(const phxqueue::scheduler::SchedulerOption &opt);
    virtual ~Scheduler() override;

    virtual phxqueue::comm::RetCode
    GetLockInfo(const phxqueue::comm::proto::GetLockInfoRequest &req,
                phxqueue::comm::proto::GetLockInfoResponse &resp) override;
    virtual phxqueue::comm::RetCode
    AcquireLock(const phxqueue::comm::proto::AcquireLockRequest &req,
                phxqueue::comm::proto::AcquireLockResponse &resp) override;
};


}  // namespace scheduler

}  // namespace phxqueue_phxrpc

