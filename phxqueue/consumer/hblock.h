#pragma once

#include <cinttypes>
#include <map>
#include <memory>
#include <string>
#include <vector>

#include "phxqueue/comm.h"

#include "phxqueue/consumer/consumer.h"


namespace phxqueue {

namespace consumer {


class HeartBeatLock {
  public:
    HeartBeatLock();
    virtual ~HeartBeatLock();

    comm::RetCode Init(Consumer *consumer, const int shmkey, const std::string &lockpath, const int nproc);
    void RunSync();
    comm::RetCode Lock(const int vpid, int &sub_id, int &store_id, int &queue_id);
    comm::RetCode GetQueuesDistribute(std::vector<Queue_t> &queues);

  protected:
    comm::RetCode Sync();
    void ClearInvalidSubIDs(const std::set<int> &valid_sub_ids);
    void DistubePendingQueues(const std::map<int, std::vector<Queue_t>> &sub_id2pending_queues);
    comm::RetCode GetAddrScale(SubID2AddrScales &sub_id2addr_scales);
    comm::RetCode GetAllQueues(const int sub_id, std::vector<Queue_t> &all_queues);
    comm::RetCode GetPendingQueues(const std::vector<Queue_t> &all_queues, const AddrScales &addr_scales, std::vector<Queue_t> &pending_queues);
    comm::RetCode DoLock(const int vpid, Queue_t *const info);
    void UpdateProcUsed();

  private:
    class HeartBeatLockImpl;
    std::unique_ptr<HeartBeatLockImpl> impl_;
};


}  // namespace consumer

}  // namespace phxqueue

