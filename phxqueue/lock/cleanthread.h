#pragma once

#include "phxpaxos/node.h"

#include "phxqueue/lock/proto/lock.pb.h"
#include "phxqueue/lock/lockdb.h"
#include "phxqueue/lock/lockmgr.h"


namespace phxqueue {

namespace lock {


class CleanThread {
  public:
    CleanThread(Lock *const lock);
    virtual ~CleanThread();

    void Run();
    void Stop();

  private:
    void DoRun();

    void CleanLock(const int paxos_group_id, const uint64_t now,
                   int &nr_group_key, int &nr_group_clean_key);

    // if no request at all, we should write checkpoint sometimes
    // there are other paxos log need to be converted to mirror, such as 'try be master' paxos log
    void WriteRestartCheckpoint(const int paxos_group_id, const uint64_t now);

    comm::RetCode ProposeCleanLock(const int paxos_group_id,
                                   const std::vector<proto::LockKeyInfo> &lock_key_infos);

    class CleanThreadImpl;
    std::unique_ptr<CleanThreadImpl> impl_;
    uint64_t last_clean_lock_ms_{0};
    uint64_t last_write_restart_checkpoint_ms_{0};
};


}  // namespace lock

}  // namespace phxqueue

