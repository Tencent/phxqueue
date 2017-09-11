#pragma once

#include "phxqueue/lock/lock.h"


namespace phxqueue {

namespace lock {


class KeepMasterThread {
  public:
    KeepMasterThread(Lock *lock);
    virtual ~KeepMasterThread();

    void Run();
    void Stop();

  private:
    void DoRun();
    comm::RetCode InitMasterRate();
    comm::RetCode AdjustMasterRate();
    comm::RetCode KeepMaster();
    //comm::RetCode GetIdxInGroupAndGroupSize(int &idx_in_group, int &group_size);
    comm::RetCode ProposeMaster(const int paxos_group_id, const comm::proto::Addr addr);
    comm::RetCode UpdatePaxosArgs();

    class KeepMasterThreadImpl;
    std::unique_ptr<KeepMasterThreadImpl> impl_;
};


}  // namespace lock

}  // namespace phxqueue

