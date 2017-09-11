#pragma once

#include <atomic>
#include <time.h>
#include <unistd.h>

#include "phxqueue/config.h"
#include "phxqueue/consumer.h"

#include "phxqueue/store/store.h"


namespace phxqueue {

namespace store {


class KeepSyncThread {
  public:
    KeepSyncThread(Store *const store);
    virtual ~KeepSyncThread();

    void Run();
    void Stop();

  private:
    void DoRun();
    void ClearSyncCtrl();
    void MakeCheckPoint();
    void SyncCursorID();
    void Replay();
    void ReportBacklog();
    void ReportDelayStat();

    void GetAllLocalQueue(std::vector<consumer::Queue_t> &queues);
    bool QueueNeedReplay(const consumer::Queue_t &queue, const std::set<int> &pub_ids,
            const std::vector<std::unique_ptr<config::proto::ReplayInfo>> &replay_infos,
            int &replay_infos_idx);

    comm::RetCode DumpCheckPoint(const int paxos_group_id, const uint64_t cp);

    class KeepSyncThreadImpl;
    std::unique_ptr<KeepSyncThreadImpl> impl_;
};


}  // namespace store

}  // namespace phxqueue

