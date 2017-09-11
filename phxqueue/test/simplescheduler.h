#pragma once

#include <memory>

#include "phxqueue/scheduler.h"


namespace phxqueue {

namespace test {


class SimpleScheduler : public scheduler::Scheduler {
  public:
    SimpleScheduler(const scheduler::SchedulerOption &opt) : scheduler::Scheduler(opt) {}
    virtual ~SimpleScheduler() override = default;

  protected:
    virtual comm::RetCode GetLockInfo(const comm::proto::GetLockInfoRequest &req,
                                      comm::proto::GetLockInfoResponse &resp) override;
    virtual comm::RetCode AcquireLock(const comm::proto::AcquireLockRequest &req,
                                      comm::proto::AcquireLockResponse &resp) override;
};


}  // namespace test

}  // namespace phxqueue

