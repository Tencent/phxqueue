#pragma once

#include <atomic>

#include "phxpaxos/options.h"

#include "phxqueue/comm.h"
#include "phxqueue/lock/lock.h"

namespace phxqueue {

namespace lock {


class LockSM : public phxpaxos::StateMachine {
  public:
    static constexpr int ID = 1;

    LockSM(Lock *const lock, const std::string &mirror_dir_path);
    virtual ~LockSM() override;

    virtual const int SMID() const override { return ID; }

    virtual bool Execute(const int paxos_group_id, const uint64_t instance_id,
                         const std::string &paxos_value, phxpaxos::SMCtx *sm_ctx) override;

    virtual bool ExecuteForCheckpoint(const int paxos_group_id, const uint64_t instance_id,
                                      const std::string &paxos_value) override;

    virtual const uint64_t GetCheckpointInstanceID(const int paxos_group_id) const override;

    virtual int GetCheckpointState(const int paxos_group_id, std::string &dir_path,
                                   std::vector<std::string> &file_list) override;

    virtual int LoadCheckpointState(const int paxos_group_id,
                                    const std::string &checkpoint_tmp_file_dir_path,
                                    const std::vector<std::string> &file_list,
                                    const uint64_t checkpoint_instance_id) override;

    virtual int LockCheckpointState() override { return 0; }

    virtual void UnLockCheckpointState() override {}

  private:
    class LockSMImpl;
    std::unique_ptr<LockSMImpl> impl_;
    uint64_t last_sync_ms_{0};
};


}  // namespace lock

}  // namespace phxqueue

