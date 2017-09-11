#pragma once

#include "phxqueue/scheduler/scheduler.h"


namespace phxqueue {

namespace scheduler {


class KeepMasterThread {
  public:
    KeepMasterThread(Scheduler *const scheduler);
    virtual ~KeepMasterThread();

    void Run();
    void Stop();

    comm::RetCode InitMasterRate();
    comm::RetCode AdjustMasterRate();
    comm::RetCode KeepMaster();

  private:
    void DoRun();
    comm::RetCode GetLockID(int &lock_id);

    class KeepMasterThreadImpl;
    std::unique_ptr<KeepMasterThreadImpl> impl_;
};


}  // namespace scheduler

}  // namespace phxqueue

