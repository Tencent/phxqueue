#pragma once

#include <time.h>
#include <unistd.h>

#include "phxqueue/store/store.h"


namespace phxqueue {

namespace store {


class KeepMasterThread {
  public:
    KeepMasterThread(Store *const store);
    virtual ~KeepMasterThread();

    void Run();
    void Stop();

  private:
    void DoRun();
    void InitMasterRate();
    void AdjustMasterRate();
    void KeepMaster();
    void UpdatePaxosArgs();

    class KeepMasterThreadImpl;
    std::unique_ptr<KeepMasterThreadImpl> impl_;
};


}  // namespace store

}  // namespace phxqueue

