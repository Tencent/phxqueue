#pragma once

#include "phxqueue/scheduler/scheduler.h"


namespace phxqueue {

namespace scheduler {


class LoadBalanceThread {
  public:
    LoadBalanceThread(Scheduler *const scheduler);
    virtual ~LoadBalanceThread();

    void Run();
    void Stop();

  private:
    void DoRun();

    class LoadBalanceThreadImpl;
    std::unique_ptr<LoadBalanceThreadImpl> impl_;
};


}  // namespace scheduler

}  // namespace phxqueue

