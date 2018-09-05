/*
Tencent is pleased to support the open source community by making PhxQueue available.
Copyright (C) 2017 THL A29 Limited, a Tencent company. All rights reserved.
Licensed under the BSD 3-Clause License (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at

<https://opensource.org/licenses/BSD-3-Clause>

Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.
*/



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

    void CleanRecord(const int paxos_group_id, const uint64_t now,
                     int *const nr_group_key, int *const nr_group_clean_key);

    // if no request at all, we should write checkpoint sometimes
    // there are other paxos log need to be converted to mirror, such as 'try be master' paxos log
    void WriteRestartCheckpoint(const int paxos_group_id, const uint64_t now);

    //comm::RetCode ProposeCleanLock(const int paxos_group_id,
    //                               const std::vector<proto::LockKeyInfo> &lock_key_infos);

    class CleanThreadImpl;
    std::unique_ptr<CleanThreadImpl> impl_;
    uint64_t last_clean_lock_ms_{0};
    uint64_t last_write_restart_checkpoint_ms_{0};
};


}  // namespace lock

}  // namespace phxqueue

