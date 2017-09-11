/*
Tencent is pleased to support the open source community by making PhxQueue available.
Copyright (C) 2017 THL A29 Limited, a Tencent company. All rights reserved.
Licensed under the BSD 3-Clause License (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at

<https://opensource.org/licenses/BSD-3-Clause>

Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.
*/



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

