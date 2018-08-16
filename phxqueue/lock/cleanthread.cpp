/*
Tencent is pleased to support the open source community by making PhxQueue available.
Copyright (C) 2017 THL A29 Limited, a Tencent company. All rights reserved.
Licensed under the BSD 3-Clause License (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at

<https://opensource.org/licenses/BSD-3-Clause>

Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.
*/



#include "phxqueue/lock/cleanthread.h"

#include <cinttypes>
#include <memory>
#include <thread>
#include <unistd.h>

#include "phxqueue/comm.h"
#include "phxqueue/lock/locksm.h"


namespace {


constexpr char *CLIENT_ID_CLEAN_THREAD{"__lock__:__clean_thread__"};


}


namespace phxqueue {

namespace lock {


using namespace std;


class CleanThread::CleanThreadImpl {
  public:
    CleanThreadImpl() {}
    virtual ~CleanThreadImpl() {}

    Lock *lock{nullptr};

    unique_ptr<thread> t{nullptr};
    bool stop{true};
};


CleanThread::CleanThread(Lock *const lock) : impl_(new CleanThreadImpl()) {
    impl_->lock = lock;
}

CleanThread::~CleanThread() {
    Stop();
}

void CleanThread::Run() {
    if (!impl_->t) {
        impl_->stop = false;
        impl_->t = unique_ptr<thread>(new thread(&CleanThread::DoRun, this));
    }
    assert(impl_->t);
}

void CleanThread::Stop() {
    if (impl_->t) {
        impl_->stop = true;
        impl_->t->join();
        impl_->t.release();
    }
}

void CleanThread::DoRun() {
    if (nullptr == impl_->lock || nullptr == impl_->lock->GetNode()) {
        QLErr("lock %p", impl_->lock);

        return;
    }

    while (true) {
        if (impl_->stop) return;

        int nr_key{0};
        int nr_clean_key{0};
        const uint64_t now{comm::utils::Time::GetSteadyClockMS()};

        bool need_clean_key{false};
        if (now > last_clean_lock_ms_ + impl_->lock->GetLockOption()->clean_interval_s * 1000) {
            need_clean_key = true;
            last_clean_lock_ms_ = now;
        }

        bool need_write_restart_checkpoint{false};
        if (now > last_write_restart_checkpoint_ms_ +
            impl_->lock->GetLockOption()->idle_write_checkpoint_interval_s * 1000) {
            need_write_restart_checkpoint = true;
            last_write_restart_checkpoint_ms_ = now;
        }

        for (int paxos_group_id{0}; impl_->lock->GetLockOption()->nr_group > paxos_group_id;
             ++paxos_group_id) {

            if (need_clean_key) {
                int nr_group_key{0};
                int nr_group_clean_key{0};
                CleanRecord(paxos_group_id, now, &nr_group_key, &nr_group_clean_key);
                nr_key += nr_group_key;
                nr_clean_key += nr_group_clean_key;
            }

            if (need_write_restart_checkpoint) {
                WriteRestartCheckpoint(paxos_group_id, now);
            }
        }  // foreach group

        QLInfo("now %llu nr_key %d nr_clean_key %d", now, nr_key, nr_clean_key);
        comm::LockCleanThreadBP::GetThreadInstance()->
                OnNrKey(impl_->lock->GetTopicID(), nr_key);
        comm::LockCleanThreadBP::GetThreadInstance()->
                OnNrCleanKey(impl_->lock->GetTopicID(), nr_clean_key);

        sleep(1);
    }
}

void CleanThread::CleanRecord(const int paxos_group_id, const uint64_t now,
                              int *const nr_group_key, int *const nr_group_clean_key) {
    if (!impl_->lock->GetNode()->IsIMMaster(paxos_group_id))
        return;

    auto &&current_map(impl_->lock->GetLockMgr()->map(paxos_group_id));
    int i{0};
    (*nr_group_key) = 0;
    (*nr_group_clean_key) = 0;
    while (current_map.GetSizeRecord() > i &&
           impl_->lock->GetLockOption()->max_clean_lock_num > i) {
        ++i;

        // 1. make args
        proto::LockPaxosArgs args;

        // begin mutex
        {

            // ensure not acquiring lock or geting lock info
            comm::utils::MutexGuard guard(current_map.mutex());

            string current_loop_key;
            comm::RetCode ret{current_map.ForwardLoopKey(&current_loop_key)};
            if (comm::RetCode::RET_OK != ret) {
                QLInfo("paxos_group_id %d ForwardLoopKey err %d",
                       paxos_group_id, comm::as_integer(ret));

                continue;
            }
            proto::LocalRecordInfo local_record_info;
            ret = current_map.GetRecord(current_loop_key, local_record_info);
            if (comm::RetCode::RET_KEY_IGNORE == ret) {
                QLInfo("paxos_group_id %d lock_key \"%s\" GetRecord ignore",
                       paxos_group_id, current_loop_key.c_str());

                continue;
            }

            ++(*nr_group_key);

            if (comm::RetCode::RET_OK != ret) {
                QLErr("paxos_group_id %d lock_key \"%s\" GetRecord err %d",
                      paxos_group_id, current_loop_key.c_str(), comm::as_integer(ret));

                continue;
            }

            // add expired lock key
            // if -1 == lease_time, keep it forever
            if (-1 != local_record_info.lease_time_ms() &&
                now > local_record_info.expire_time_ms()) {
                // TODO: use move?
                auto &&lock_info(args.mutable_acquire_lock_req()->mutable_lock_info());
                lock_info->set_lock_key(current_loop_key);
                lock_info->set_version(local_record_info.version());
                lock_info->set_client_id(CLIENT_ID_CLEAN_THREAD);
                lock_info->set_lease_time_ms(0);
                QLInfo("paxos_group_id %d lock_key \"%s\" lease_time_ms %llu "
                       "now %" PRIu64 " > expire_time_ms %llu delete", paxos_group_id,
                       current_loop_key.c_str(), local_record_info.lease_time_ms(),
                       now, local_record_info.expire_time_ms());
            } else {
                // no expire to clean
                continue;
            }
        }
        // end mutex

        // 2. serialize args to paxos value
        string buf;
        args.SerializeToString(&buf);

        // 3. send to paxos
        LockContext lc;
        phxpaxos::SMCtx sm_ctx(LockSM::ID, &lc);
        uint64_t instance_id{0uLL};
        int paxos_ret{impl_->lock->GetNode()->Propose(paxos_group_id,
                                                      buf, instance_id, &sm_ctx)};
        if (phxpaxos::PaxosTryCommitRet_OK != paxos_ret) {
            QLErr("paxos_group_id %d Propose err %d buf.size %zu",
                  paxos_group_id, paxos_ret, buf.size());
            comm::LockCleanThreadBP::GetThreadInstance()->
                    OnProposeCleanErr(impl_->lock->GetTopicID(), paxos_group_id);

            continue;
        } else {
            ++(*nr_group_clean_key);
            QLInfo("paxos_group_id %d Propose ok instance_id %llu buf.size %zu",
                   paxos_group_id, instance_id, buf.size());
            comm::LockCleanThreadBP::GetThreadInstance()->
                    OnProposeCleanSucc(impl_->lock->GetTopicID(), paxos_group_id);
        }
    }
}

void CleanThread::WriteRestartCheckpoint(const int paxos_group_id, const uint64_t now) {
    uint64_t now_instance_id{impl_->lock->GetNode()->GetNowInstanceID(paxos_group_id)};
    uint64_t memory_checkpoint{impl_->lock->GetLockMgr()->memory_checkpoint(paxos_group_id)};
    uint64_t last_instance_id{impl_->lock->GetLockMgr()->last_instance_id(paxos_group_id)};

    if (last_instance_id == memory_checkpoint && now_instance_id > memory_checkpoint + 1 &&
        phxpaxos::NoCheckpoint != now_instance_id) {
        // now_instance_id may be not chosen, should write now_instance_id - 1

        // 1. write and flush mirror
        impl_->lock->GetLockMgr()->leveldb(paxos_group_id).Sync();

        // 2. do NOT write and flush disk_checkpoint here!
        // write and flush restart_checkpoint instead
        comm::RetCode ret{impl_->lock->GetLockMgr()->
                WriteRestartCheckpoint(paxos_group_id, now_instance_id - 1)};
        if (comm::RetCode::RET_OK == ret) {
            QLInfo("topic_id %d paxos_group_id %d WriteRestartCheckpoint ok memory_cp %llu"
                   " last_instance_id %llu now_instance_id %llu",
                   impl_->lock->GetTopicID(), paxos_group_id,
                   memory_checkpoint, last_instance_id, now_instance_id);
        } else {
            QLErr("topic_id %d paxos_group_id %d WriteRestartCheckpoint err %d memory_cp %llu"
                  " last_instance_id %llu now_instance_id %llu",
                  impl_->lock->GetTopicID(), paxos_group_id, comm::as_integer(ret),
                  memory_checkpoint, last_instance_id, now_instance_id);
        }
    } else {
        QLInfo("topic_id %d paxos_group_id %d WriteRestartCheckpoint skip memory_cp %llu"
               " last_instance_id %llu now_instance_id %llu",
               impl_->lock->GetTopicID(), paxos_group_id,
               memory_checkpoint, last_instance_id, now_instance_id);
    }
}

//void CleanThread::DoRun() {
//    if (nullptr == impl_->lock || nullptr == impl_->lock->GetNode()) {
//        QLErr("lock %p", impl_->lock);
//
//        return;
//    }
//
//    while (true) {
//        if (impl_->stop) return;
//
//        int nr_key{0};
//        int nr_clean_key{0};
//        const uint64_t now{phxqueue::Time::GetSteadyClockMS()};
//        for (int paxos_group_id{0}; impl_->lock->GetLockOption()->nr_group > paxos_group_id;
//             ++paxos_group_id) {
//            if (!impl_->lock->GetNode()->IsIMMaster(paxos_group_id))
//                continue;
//
//            // TODO: Not copy string
//            vector<phxqueue_proto::LockKeyInfo> lock_key_infos;
//            // ensure not acquiring lock
//            auto &&current_map(impl_->lock->GetLockMgr()->map(paxos_group_id));
//
//            // begin mutex
//            {
//
//                // ensure not acquiring lock or geting lock info
//                MutexGuard guard(current_map.mutex());
//                nr_key += current_map.GetSize();
//
//                // collect all expire key
//                for (current_map.SeekToFirst(); current_map.Valid(); current_map.Next()) {
//                    string lock_key;
//                    phxqueue_proto::LocalLockInfo local_lock_info;
//                    comm::RetCode ret{current_map.GetCurrent(lock_key, local_lock_info)};
//                    if (comm::RetCode::RET_ERR_IGNORE == ret)
//                        continue;
//
//                    if (comm::RetCode::RET_OK != ret) {
//                        QLErr("paxos_group_id %d GetCurrent err %d", paxos_group_id, comm::as_integer(ret));
//
//                        continue;
//                    }
//
//                    // add expired lock key
//                    if (now > local_lock_info.expire_time_ms()) {
//                        // TODO: use move?
//                        phxqueue_proto::LockKeyInfo lock_key_info;
//                        lock_key_info.set_version(local_lock_info.version());
//                        lock_key_info.set_lock_key(lock_key);
//                        lock_key_infos.push_back(lock_key_info);
//                        ++nr_clean_key;
//                    }
//                }  // foreach key
//
//            }
//            // end mutex
//
//            comm::RetCode ret{ProposeCleanLock(paxos_group_id, lock_key_infos)};
//            if (comm::RetCode::RET_OK != ret) {
//                QLErr("paxos_group_id %d ProposeCleanLock err %d", paxos_group_id, comm::as_integer(ret));
//            }
//
//        }  // foreach group
//
//        QLInfo("now %llu nr_key %d nr_clean_key %d", now, nr_key, nr_clean_key);
//        // TODO:
//        //OssAttrSet(mgr_->oss_attr_id(), 6u, nr_key);
//        //OssAttrSet(mgr_->oss_attr_id(), 7u, nr_clean_key);
//
//        sleep(impl_->lock->GetLockOption()->clean_interval_s);
//    }
//}

//comm::RetCode CleanThread::ProposeCleanLock(const int paxos_group_id,
//                                            const vector<proto::LockKeyInfo> &lock_key_infos) {
//    // TODO: clean one by one
//    vector<phxqueue_proto::LockKeyInfo> lock_key_infos2;
//    int max_clean_lock_num{impl_->lock->GetLockOption()->max_clean_lock_num};
//    lock_key_infos2.reserve(max_clean_lock_num);
//    for (auto &&lock_key_info : lock_key_infos) {
//        // TODO: use move?
//        lock_key_infos2.push_back(lock_key_info);
//        if (lock_key_infos2.size() >= max_clean_lock_num) {
//            phxqueue_proto::LockPaxosArgs args;
//
//            // 1. make args
//            for (auto &&lock_key_info : lock_key_infos2) {
//                phxqueue_proto::LockKeyInfo *new_lock_key_info{args.mutable_clean_lock_req()->add_lock_key_infos()};
//                new_lock_key_info->set_version(lock_key_info.version());
//                new_lock_key_info->set_lock_key(lock_key_info.lock_key());
//            }
//
//            // 2. serialize args to paxos value
//            string buf;
//            args.SerializeToString(&buf);
//
//            // 3. send to paxos
//            LockContext lc;
//            phxpaxos::SMCtx sm_ctx(LockSM::ID, &lc);
//            uint64_t instance_id{0};
//            int paxos_ret{impl_->lock->GetNode()->Propose(paxos_group_id, buf, instance_id, &sm_ctx)};
//            if (phxpaxos::PaxosTryCommitRet_OK != paxos_ret) {
//                QLErr("paxos_group_id %d Propose err %d buf.size %zu", paxos_group_id, paxos_ret, buf.size());
//                // TODO:
//                //OssAttrInc(mgr_->oss_attr_id(), 107u, 1u);
//
//                continue;
//            } else {
//                QLInfo("paxos_group_id %d Propose ok instance_id %llu buf.size %zu",
//                       paxos_group_id, instance_id, buf.size());
//                // TODO:
//                //OssAttrInc(mgr_->oss_attr_id(), 106u, 1u);
//            }
//
//            // 4. reset
//            lock_key_infos2.clear();
//        }
//    }
//
//    return comm::RetCode::RET_OK;
//}


}  // namespace lock

}  // namespace phxqueue

