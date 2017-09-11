#pragma once

#include <cinttypes>
#include <pthread.h>
#include <vector>

#include "phxqueue/comm/errdef.h"


namespace phxqueue {

namespace comm {


class MultiProc {
  public:
    MultiProc() {}
    virtual ~MultiProc() {}

    // watch and refork the child process if killed/aborted unexpectedly
    void ForkAndRun(const int procs);

  protected:
    virtual void ChildRun(const int vpid) = 0;

  private:
    std::vector<pid_t> children_;
};


}  // namespace comm

}  // namespace phxqueue

