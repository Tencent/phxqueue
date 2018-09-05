/*
Tencent is pleased to support the open source community by making PhxQueue available.
Copyright (C) 2017 THL A29 Limited, a Tencent company. All rights reserved.
Licensed under the BSD 3-Clause License (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at

<https://opensource.org/licenses/BSD-3-Clause>

Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.
*/



#include "phxqueue/lock/lockmgr.h"

#include <string>

#include "phxqueue/comm.h"
#include "phxqueue/config.h"

#include "phxqueue/lock/lockdb.h"
#include "phxqueue/lock/locksm.h"
#include "phxqueue/lock/lockutils.h"


namespace {


constexpr char *KEY_IGNORE_CHECKPOINT{"__ignore__:__checkpoint__"};
constexpr char *KEY_IGNORE_RESTART_CHECKPOINT{"__ignore__:__restart_checkpoint__"};
constexpr char *KEY_IGNORE_CHECKPOINT_DEPRECATED{"__ignore__.__checkpoint__"};
constexpr char *KEY_IGNORE_RESTART_CHECKPOINT_DEPRECATED{"__ignore__.__restart_checkpoint__"};


}  // namespace


namespace phxqueue {

namespace lock {


using namespace phxpaxos;
using namespace std;


class LockMgr::LockMgrImpl {
  public:
    LockMgrImpl() {}
    virtual ~LockMgrImpl() {}

    Lock *lock{nullptr};
    GroupVector groups;
};


LockMgr::LockMgr(Lock *const lock) : impl_(new LockMgrImpl()) {
    impl_->lock = lock;
}

LockMgr::~LockMgr() {}

comm::RetCode LockMgr::Init(const string &mirror_dir_path) {
    impl_->groups.reserve(impl_->lock->GetLockOption()->nr_group);
    for (int paxos_group_id{0};
         impl_->lock->GetLockOption()->nr_group > paxos_group_id; ++paxos_group_id) {
        impl_->groups.emplace_back();
        auto &&group(impl_->groups[paxos_group_id]);

        // recover leveldb
        string group_directory{mirror_dir_path + to_string(paxos_group_id)};
        comm::RetCode ret{group.leveldb.Init(LockDb::StorageType::LEVELDB, group_directory)};
        if (comm::RetCode::RET_OK != ret) {
            QLErr("paxos_group_id %d init leveldb err %d path \"%s\"", paxos_group_id, ret, group_directory.c_str());

            assert(false);
        }

        // init map
        ret = group.map.Init(LockDb::StorageType::MAP, "");
        if (comm::RetCode::RET_OK != ret) {
            QLErr("paxos_group_id %d init map err %d", paxos_group_id, ret);

            assert(false);
        }

        // recover map
        for (group.leveldb.DiskSeekToFirst(); group.leveldb.DiskValid(); group.leveldb.DiskNext()) {
            string key;
            string value;
            LockDb::DataType data_type{LockDb::DataType::NONE};
            comm::RetCode ret{group.leveldb.DiskGetCurrent(&key, &value, &data_type)};
            if (comm::RetCode::RET_OK != ret) {
                QLErr("paxos_group_id %d key \"%s\" DiskGetCurrent err %d", paxos_group_id, key.c_str(), ret);

                assert(false);
            }

            if (LockDb::DataType::IGNORE != data_type) {
                proto::LocalRecordInfo local_record_info;
                bool succ{local_record_info.ParseFromString(value)};
                if (!succ) {
                    QLErr("ParseFromString key \"%s\" value.size \"%s\" err", key.c_str(), value.size());

                    assert(false);
                }

                // reset expire time
                local_record_info.set_expire_time_ms(comm::utils::Time::GetSteadyClockMS() +
                                                     local_record_info.lease_time_ms());
                QLInfo("paxos_group_id %d key \"%s\" SetRecord", paxos_group_id, key.c_str());
                group.map.SetRecord(move(key), move(local_record_info));
            } else {
                QLInfo("paxos_group_id %d key \"%s\" ignore", paxos_group_id, key.c_str());
            }
        }

        // recover disk_checkpoint
        uint64_t disk_checkpoint{phxpaxos::NoCheckpoint};
        ret = ReadDiskCheckpoint(paxos_group_id, disk_checkpoint);
        if (comm::RetCode::RET_ERR_KEY_NOT_EXIST == ret) {
            QLErr("topic_id %d paxos_group_id %d ReadDiskCheckpoint not exist",
                  impl_->lock->GetTopicID(), paxos_group_id);
        } else if (comm::RetCode::RET_OK == ret) {
            QLInfo("topic_id %d paxos_group_id %d ReadDiskCheckpoint disk_cp %llu ok",
                   impl_->lock->GetTopicID(), paxos_group_id, disk_checkpoint);
        } else {
            QLErr("topic_id %d paxos_group_id %d ReadDiskCheckpoint err %d",
                  impl_->lock->GetTopicID(), paxos_group_id, ret);

            assert(false);
        }

        // read restart_checkpoint
        uint64_t restart_checkpoint{phxpaxos::NoCheckpoint};
        ret = ReadRestartCheckpoint(paxos_group_id, restart_checkpoint);
        if (comm::RetCode::RET_ERR_KEY_NOT_EXIST == ret) {
            QLErr("topic_id %d paxos_group_id %d ReadRestartCheckpoint not exist",
                  impl_->lock->GetTopicID(), paxos_group_id);
        } else if (comm::RetCode::RET_OK == ret) {
            QLInfo("topic_id %d paxos_group_id %d ReadRestartCheckpoint restart_cp %llu ok",
                   impl_->lock->GetTopicID(), paxos_group_id, restart_checkpoint);
        }

        NLInfo("topic_id %d paxos_group_id %d disk_cp %llu restart_cp %llu",
               impl_->lock->GetTopicID(), paxos_group_id, disk_checkpoint, restart_checkpoint);
        // if restart_checkpoint > disk_checkpoint, over write
        if (phxpaxos::NoCheckpoint != restart_checkpoint &&
            (phxpaxos::NoCheckpoint == disk_checkpoint || restart_checkpoint > disk_checkpoint)) {
            //impl_->groups[paxos_group_id].set_disk_checkpoint(restart_checkpoint);
            ret = WriteDiskCheckpoint(paxos_group_id, restart_checkpoint);
            if (comm::RetCode::RET_OK == ret) {
                NLInfo("topic_id %d paxos_group_id %d WriteDiskCheckpoint restart_cp %llu ok",
                       impl_->lock->GetTopicID(), paxos_group_id, restart_checkpoint);
            } else {
                NLErr("topic_id %d paxos_group_id %d WriteDiskCheckpoint restart_cp %llu err %d",
                      impl_->lock->GetTopicID(), paxos_group_id, restart_checkpoint, ret);
            }
        }

        // recover last_instance_id
        impl_->groups[paxos_group_id].set_last_instance_id(impl_->groups[paxos_group_id].disk_checkpoint());

    }  // foreach group

    return comm::RetCode::RET_OK;
}

comm::RetCode LockMgr::Dispose() {
    comm::RetCode ret{comm::RetCode::RET_ERR_UNINIT};

    for (int i{0}; impl_->groups.size() > i; ++i) {
        // dispose leveldb
        ret = impl_->groups[i].leveldb.Dispose();
        if (comm::RetCode::RET_OK != ret) {
            QLErr("dispose leveldb err %d", ret);
        }

        // dispose map
        ret = impl_->groups[i].map.Dispose();
        if (comm::RetCode::RET_OK != ret) {
            QLErr("dispose map err %d", ret);
        }
    }

    return ret;
}

comm::RetCode LockMgr::ReadDiskCheckpoint(const GroupVector::size_type paxos_group_id,
                                          uint64_t &disk_checkpoint) {
    comm::LockMgrBP::GetThreadInstance()->
            OnReadCheckpoint(impl_->lock->GetTopicID(), paxos_group_id);

    string vstr;
    comm::RetCode ret{leveldb(paxos_group_id).DiskGet(KEY_IGNORE_CHECKPOINT, vstr)};

    if (comm::RetCode::RET_ERR_KEY_NOT_EXIST == ret) {
        // TODO: deprecated
        ret = leveldb(paxos_group_id).DiskGet(KEY_IGNORE_CHECKPOINT_DEPRECATED, vstr);
        if (comm::RetCode::RET_ERR_KEY_NOT_EXIST == ret) {
            disk_checkpoint = phxpaxos::NoCheckpoint;
            QLErr("topic_id %d paxos_group_id %d not exist",
                  impl_->lock->GetTopicID(), paxos_group_id);
        } else if (comm::RetCode::RET_OK == ret) {
            disk_checkpoint = strtoul(vstr.c_str(), nullptr, 10);
            QLInfo("topic_id %d paxos_group_id %d cp %llu ok",
                  impl_->lock->GetTopicID(), paxos_group_id, disk_checkpoint);
        } else {
            disk_checkpoint = phxpaxos::NoCheckpoint;
            QLErr("topic_id %d paxos_group_id %d err %d",
                  impl_->lock->GetTopicID(), paxos_group_id, ret);
        }
    } else if (comm::RetCode::RET_OK == ret) {
        disk_checkpoint = strtoul(vstr.c_str(), nullptr, 10);
        QLInfo("topic_id %d paxos_group_id %d cp %llu ok",
               impl_->lock->GetTopicID(), paxos_group_id, disk_checkpoint);
    } else {
        disk_checkpoint = phxpaxos::NoCheckpoint;
        QLErr("topic_id %d paxos_group_id %d err %d",
              impl_->lock->GetTopicID(), paxos_group_id, ret);
    }

    impl_->groups[paxos_group_id].set_disk_checkpoint(disk_checkpoint);
    // assign to memory checkpoint as well
    impl_->groups[paxos_group_id].set_memory_checkpoint(disk_checkpoint);

    return ret;
}

comm::RetCode LockMgr::WriteDiskCheckpoint(const GroupVector::size_type paxos_group_id,
                                           const uint64_t disk_checkpoint) {
    comm::LockMgrBP::GetThreadInstance()->
            OnWriteCheckpoint(impl_->lock->GetTopicID(), paxos_group_id, disk_checkpoint);

    if (phxpaxos::NoCheckpoint != disk_checkpoint) {
        comm::RetCode ret{leveldb(paxos_group_id).DiskSet(KEY_IGNORE_CHECKPOINT,
                                                          to_string(disk_checkpoint) , true)};

        if (comm::RetCode::RET_OK == ret) {
            QLInfo("topic_id %d paxos_group_id %d cp %llu ok",
                   impl_->lock->GetTopicID(), paxos_group_id, disk_checkpoint);

            impl_->groups[paxos_group_id].set_disk_checkpoint(disk_checkpoint);
            // assign to memory checkpoint as well
            impl_->groups[paxos_group_id].set_memory_checkpoint(disk_checkpoint);
        } else {
            QLErr("topic_id %d paxos_group_id %d cp %llu err %d",
                  impl_->lock->GetTopicID(), paxos_group_id, disk_checkpoint, ret);

            return ret;
        }
    }

    return comm::RetCode::RET_OK;
}

comm::RetCode LockMgr::ReadRestartCheckpoint(const GroupVector::size_type paxos_group_id,
                                             uint64_t &restart_checkpoint) {
    comm::LockMgrBP::GetThreadInstance()->
            OnReadRestartCheckpoint(impl_->lock->GetTopicID(), paxos_group_id);

    string vstr;
    comm::RetCode ret{leveldb(paxos_group_id).DiskGet(KEY_IGNORE_RESTART_CHECKPOINT, vstr)};

    if (comm::RetCode::RET_ERR_KEY_NOT_EXIST == ret) {
        ret = leveldb(paxos_group_id).DiskGet(KEY_IGNORE_RESTART_CHECKPOINT_DEPRECATED, vstr);
        if (comm::RetCode::RET_ERR_KEY_NOT_EXIST == ret) {
            restart_checkpoint = phxpaxos::NoCheckpoint;
            QLErr("topic_id %d paxos_group_id %d not exist",
                  impl_->lock->GetTopicID(), paxos_group_id);
        } else if (comm::RetCode::RET_OK == ret) {
            restart_checkpoint = strtoul(vstr.c_str(), nullptr, 10);
            QLInfo("topic_id %d paxos_group_id %d restart_cp %llu ok",
                  impl_->lock->GetTopicID(), paxos_group_id, restart_checkpoint);
        } else {
            restart_checkpoint = phxpaxos::NoCheckpoint;
            QLErr("topic_id %d paxos_group_id %d err %d",
                  impl_->lock->GetTopicID(), paxos_group_id, ret);
        }
    } else if (comm::RetCode::RET_OK == ret) {
        restart_checkpoint = strtoul(vstr.c_str(), nullptr, 10);
        QLInfo("topic_id %d paxos_group_id %d restart_cp %llu ok",
               impl_->lock->GetTopicID(), paxos_group_id, restart_checkpoint);
    } else {
        restart_checkpoint = phxpaxos::NoCheckpoint;
        QLErr("topic_id %d paxos_group_id %d err %d",
              impl_->lock->GetTopicID(), paxos_group_id, ret);
    }

    return ret;
}

comm::RetCode LockMgr::WriteRestartCheckpoint(const GroupVector::size_type paxos_group_id,
                                              const uint64_t restart_checkpoint) {
    comm::LockMgrBP::GetThreadInstance()->
            OnWriteRestartCheckpoint(impl_->lock->GetTopicID(),
                                     paxos_group_id, restart_checkpoint);

    if (phxpaxos::NoCheckpoint != restart_checkpoint) {
        comm::RetCode ret{leveldb(paxos_group_id).DiskSet(KEY_IGNORE_RESTART_CHECKPOINT,
                                                          to_string(restart_checkpoint) , true)};

        if (comm::RetCode::RET_OK == ret) {
            QLInfo("topic_id %d paxos_group_id %d restart_cp %llu ok",
                   impl_->lock->GetTopicID(), paxos_group_id, restart_checkpoint);
        } else {
            QLErr("topic_id %d paxos_group_id %d restart_cp %llu err %d",
                  impl_->lock->GetTopicID(), paxos_group_id, restart_checkpoint, ret);
        }

        return ret;
    }

    return comm::RetCode::RET_OK;
}

LockDb &LockMgr::map(const GroupVector::size_type paxos_group_id) {
    return impl_->groups[paxos_group_id].map;
}

const LockDb &LockMgr::map(const GroupVector::size_type paxos_group_id) const {
    return impl_->groups.at(paxos_group_id).map;
}

LockDb &LockMgr::leveldb(const GroupVector::size_type paxos_group_id) {
    return impl_->groups[paxos_group_id].leveldb;
}

const LockDb &LockMgr::leveldb(const GroupVector::size_type paxos_group_id) const {
    return impl_->groups.at(paxos_group_id).leveldb;
}

uint64_t LockMgr::last_instance_id(const GroupVector::size_type paxos_group_id) const {
    return impl_->groups.at(paxos_group_id).last_instance_id();
}

void LockMgr::set_last_instance_id(const GroupVector::size_type paxos_group_id,
                                   const uint64_t instance_id) {
    impl_->groups[paxos_group_id].set_last_instance_id(instance_id);
}

uint64_t LockMgr::memory_checkpoint(const GroupVector::size_type paxos_group_id) const {
    return impl_->groups.at(paxos_group_id).memory_checkpoint();
}

void LockMgr::set_memory_checkpoint(const GroupVector::size_type paxos_group_id,
                                    const uint64_t memory_checkpoint) {
    impl_->groups[paxos_group_id].set_memory_checkpoint(memory_checkpoint);
}

uint64_t LockMgr::disk_checkpoint(const GroupVector::size_type paxos_group_id) const {
    return impl_->groups.at(paxos_group_id).disk_checkpoint();
}


}  // namespace lock

}  // namespace phxqueue

