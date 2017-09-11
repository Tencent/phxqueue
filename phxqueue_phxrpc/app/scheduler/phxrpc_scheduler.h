#pragma once

#include "phxqueue/scheduler.h"


namespace phxqueue_phxrpc {

namespace scheduler {


class PhxRpcScheduler : public phxqueue::scheduler::Scheduler {
  public:
    PhxRpcScheduler(const phxqueue::scheduler::SchedulerOption &opt);
    virtual ~PhxRpcScheduler();

  protected:
    virtual phxqueue::comm::RetCode GetLockInfo(const phxqueue::comm::proto::GetLockInfoRequest &req,
                                          phxqueue::comm::proto::GetLockInfoResponse &resp) override;
    virtual phxqueue::comm::RetCode AcquireLock(const phxqueue::comm::proto::AcquireLockRequest &req,
                                          phxqueue::comm::proto::AcquireLockResponse &resp) override;
};


}

}

