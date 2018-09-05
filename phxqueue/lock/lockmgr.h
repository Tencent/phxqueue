/*
Tencent is pleased to support the open source community by making PhxQueue available.
Copyright (C) 2017 THL A29 Limited, a Tencent company. All rights reserved.
Licensed under the BSD 3-Clause License (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at

<https://opensource.org/licenses/BSD-3-Clause>

Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.
*/



#pragma once

#include "phxpaxos/sm.h"

#include "phxqueue/comm.h"
#include "phxqueue/lock/lockoption.h"
#include "phxqueue/lock/lockdb.h"


namespace phxqueue {

namespace lock {


class LockMgr;

struct GroupData {
  public:
    LockDb map;
    LockDb leveldb;
    uint64_t last_instance_id() { return last_instance_id_; }
    uint64_t memory_checkpoint() { return memory_checkpoint_; }
    uint64_t disk_checkpoint() { return disk_checkpoint_; }

  private:
    friend class LockMgr;

    void set_last_instance_id(const uint64_t instance_id) { last_instance_id_ = instance_id; }
    void set_memory_checkpoint(const uint64_t memory_checkpoint) { memory_checkpoint_ = memory_checkpoint; }
    void set_disk_checkpoint(const uint64_t disk_checkpoint) { disk_checkpoint_ = disk_checkpoint; }

    uint64_t last_instance_id_{phxpaxos::NoCheckpoint};
    uint64_t memory_checkpoint_{phxpaxos::NoCheckpoint};
    uint64_t disk_checkpoint_{phxpaxos::NoCheckpoint};
};

typedef std::vector<GroupData> GroupVector;


class LockMgr {
  public:
    LockMgr(Lock *const lock);
    virtual ~LockMgr();

    comm::RetCode Init(const std::string &mirror_dir_path);
    comm::RetCode Dispose();

    comm::RetCode ReadDiskCheckpoint(const GroupVector::size_type paxos_group_id,
                                     uint64_t &checkpoint);
    comm::RetCode WriteDiskCheckpoint(const GroupVector::size_type paxos_group_id,
                                      const uint64_t checkpoint);

    comm::RetCode ReadRestartCheckpoint(const GroupVector::size_type paxos_group_id,
                                        uint64_t &restart_checkpoint);
    comm::RetCode WriteRestartCheckpoint(const GroupVector::size_type paxos_group_id,
                                         const uint64_t checkpoint);

    LockDb &map(const GroupVector::size_type paxos_group_id);
    const LockDb &map(const GroupVector::size_type paxos_group_id) const;

    LockDb &leveldb(const GroupVector::size_type paxos_group_id);
    const LockDb &leveldb(const GroupVector::size_type paxos_group_id) const;

    uint64_t last_instance_id(const GroupVector::size_type paxos_group_id) const;
    void set_last_instance_id(const GroupVector::size_type paxos_group_id,
                              const uint64_t instance_id);
    uint64_t disk_checkpoint(const GroupVector::size_type paxos_group_id) const;
    uint64_t memory_checkpoint(const GroupVector::size_type paxos_group_id) const;
    void set_memory_checkpoint(const GroupVector::size_type paxos_group_id,
                               const uint64_t memory_checkpoint);

  private:
    class LockMgrImpl;
    std::unique_ptr<LockMgrImpl> impl_;
};


}  // namespace lock

}  // namespace phxqueue

