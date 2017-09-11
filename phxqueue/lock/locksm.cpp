#include "locksm.h"

#include <cinttypes>

#include "phxpaxos/node.h"

#include "phxqueue/comm.h"
#include "phxqueue/lock/lock.h"
#include "phxqueue/lock/lockdb.h"
#include "phxqueue/lock/lockmgr.h"
#include "phxqueue/lock/lockutils.h"

using namespace std;


namespace {


using namespace phxqueue;
using namespace phxqueue::lock;


bool DoExecute(Lock *lock, LockDb::Type db_type, const int paxos_group_id,
               const uint64_t instance_id, const proto::LockPaxosArgs &args,
               const bool sync = false, phxpaxos::SMCtx *sm_ctx = nullptr) {
    LockContext *lc{nullptr};
    if (sm_ctx && sm_ctx->m_pCtx) {
        // only master set ctx
        lc = static_cast<LockContext *>(sm_ctx->m_pCtx);
    }

    if (!lock || !lock->GetLockMgr()) {
        NLErr("topic_id %d paxos_group_id %d instance_id %llu lock_mgr null",
              lock->GetTopicID(), paxos_group_id, instance_id);
        if (lc)
            lc->result = comm::RetCode::RET_ERR_LOGIC;

        return false;
    }

    NLVerb("topic_id %d paxos_group_id %d instance_id %llu",
           lock->GetTopicID(), paxos_group_id, instance_id);

    if (args.has_acquire_lock_req()) {
        NLInfo("topic_id %d paxos_group_id %d instance_id %llu acquire lock sync %d",
               lock->GetTopicID(), paxos_group_id, instance_id, sync);
        auto &&lock_info(args.acquire_lock_req().lock_info());

        proto::LockKeyInfo lock_key_info;
        lock_key_info.set_version(lock_info.version());
        lock_key_info.set_lock_key(lock_info.lock_key());

        proto::LocalLockInfo local_lock_info;
        local_lock_info.set_version(instance_id);
        local_lock_info.set_client_id(lock_info.client_id());
        local_lock_info.set_lease_time_ms(lock_info.lease_time_ms());
        local_lock_info.set_expire_time_ms(comm::utils::Time::GetSteadyClockMS() + lock_info.lease_time_ms());

        comm::RetCode ret{comm::RetCode::RET_ERR_UNINIT};
        if (LockDb::Type::MAP == db_type) {
            comm::utils::MutexGuard guard(lock->GetLockMgr()->map(paxos_group_id).mutex());
            ret = lock->GetLockMgr()->map(paxos_group_id).
                    AcquireLock(lock_key_info, local_lock_info);

            // update last instance id
            lock->GetLockMgr()->set_last_instance_id(paxos_group_id, instance_id);
            NLInfo("topic_id %d paxos_group_id %d last_instance_id %llu",
                   lock->GetTopicID(), paxos_group_id, instance_id);
        } else if (LockDb::Type::LEVELDB == db_type) {
            ret = lock->GetLockMgr()->leveldb(paxos_group_id).
                    AcquireLock(lock_key_info, local_lock_info, sync);

            // update checkpoint
            if (comm::RetCode::RET_OK == ret && sync) {
                ret = lock->GetLockMgr()->WriteCheckpoint(paxos_group_id, instance_id);
                if (comm::RetCode::RET_OK == ret) {
                    NLInfo("topic_id %d paxos_group_id %d WriteCheckpoint cp %llu ok",
                           lock->GetTopicID(), paxos_group_id, instance_id);
                } else {
                    NLErr("topic_id %d paxos_group_id %d WriteCheckpoint cp %llu err %d",
                          lock->GetTopicID(), paxos_group_id, instance_id, ret);
                }
            }
        } else {
            ret = comm::RetCode::RET_ERR_NO_IMPL;
        }
        if (comm::RetCode::RET_OK != ret) {
            NLErr("topic_id %d paxos_group_id %d instance_id %llu Put err %d",
                  lock->GetTopicID(), paxos_group_id, instance_id, ret);
            if (lc)
                lc->result = ret;
        } else {
            NLInfo("topic_id %d paxos_group_id %d instance_id %llu Put ok",
                   lock->GetTopicID(), paxos_group_id, instance_id);
            if (lc)
                lc->result = comm::RetCode::RET_OK;
        }

    }
    // may be drop master request
    //else {
    //    comm::RetCode ret{comm::RetCode::RET_ERR_NO_IMPL};
    //    NLErr("topic_id %d paxos_group_id %d instance_id %llu unknown paxos_args err",
    //          lock->GetTopicID(), paxos_group_id, instance_id);
    //    if (lc)
    //        lc->result = ret;
    //}

    return true;
}


}  // namespace


namespace phxqueue {

namespace lock {


class LockSM::LockSMImpl {
  public:
    LockSMImpl() {}
    virtual ~LockSMImpl() {}

    Lock *lock{nullptr};
    string mirror_dir_path;
};

LockSM::LockSM(Lock *const lock, const string &mirror_dir_path)
        : impl_(new LockSMImpl()){
    impl_->lock = lock;
    impl_->mirror_dir_path = mirror_dir_path;
}

LockSM::~LockSM() {}

bool LockSM::Execute(const int paxos_group_id, const uint64_t instance_id,
                     const string &paxos_value, phxpaxos::SMCtx *sm_ctx) {
    comm::LockSMBP::GetThreadInstance()->
            OnExecute(impl_->lock->GetTopicID(), paxos_group_id,
                      instance_id, paxos_value);
    QLVerb("topic_id %d paxos_group_id %d instance_id %llu",
           impl_->lock->GetTopicID(), paxos_group_id, instance_id);

    proto::LockPaxosArgs args;
    bool succ{args.ParseFromString(paxos_value)};
    if (!succ) {
        QLErr("topic_id %d paxos_group_id %d instance_id %llu ParseFromString err",
              impl_->lock->GetTopicID(), paxos_group_id, instance_id);

        return false;
    }

    int ret{DoExecute(impl_->lock, LockDb::Type::MAP, paxos_group_id,
                      instance_id, args, false, sm_ctx)};

    if (!args.master_addr().ip().empty()) {
        auto opt(impl_->lock->GetLockOption());

        comm::proto::Addr addr;
        addr.set_ip(opt->ip);
        addr.set_port(opt->port);
        addr.set_paxos_port(opt->paxos_port);

        if (!(addr.ip() == args.master_addr().ip() &&
              addr.port() == args.master_addr().port() &&
              addr.paxos_port() == args.master_addr().paxos_port())) {
            QLInfo("topic_id %d paxos_group_id %d DropMaster self_addr %s master_addr %s",
                   impl_->lock->GetTopicID(), paxos_group_id, addr.ip().c_str(),
                   args.master_addr().ip().c_str());
            auto &&node(impl_->lock->GetNode());
            if (node) {
                node->DropMaster(paxos_group_id);
            } else {
                // init should not drop master
                QLInfo("topic_id %d paxos_group_id %d node null self_addr %s master_addr %s",
                       impl_->lock->GetTopicID(), paxos_group_id, addr.ip().c_str(),
                       args.master_addr().ip().c_str());
            }
        }
    }

    return ret;
}

bool LockSM::ExecuteForCheckpoint(const int paxos_group_id, const uint64_t instance_id,
                                  const string &paxos_value) {
    comm::LockSMBP::GetThreadInstance()->
            OnExecuteForCheckpoint(impl_->lock->GetTopicID(), paxos_group_id,
                                   instance_id, paxos_value);

    if (impl_->lock->GetLockOption()->no_leveldb)
        return true;

    const uint64_t checkpoint{impl_->lock->GetLockMgr()->checkpoint(paxos_group_id)};
    bool sync{instance_id - checkpoint >= impl_->lock->GetLockOption()->checkpoint_interval};
    if (!sync) {
        const uint64_t now{comm::utils::Time::GetSteadyClockMS()};
        if (now > last_sync_ms_ + 60000) {
            sync = true;
        }
    }
    if (sync) {
        last_sync_ms_ = comm::utils::Time::GetSteadyClockMS();

        comm::LockSMBP::GetThreadInstance()->
                OnExecuteForCheckpointSync(impl_->lock->GetTopicID(), paxos_group_id,
                                           instance_id, paxos_value);
    } else {
        comm::LockSMBP::GetThreadInstance()->
                OnExecuteForCheckpointNoSync(impl_->lock->GetTopicID(), paxos_group_id,
                                             instance_id, paxos_value);
    }
    QLVerb("topic_id %d paxos_group_id %d instance_id %llu cp %llu sync %d",
           impl_->lock->GetTopicID(), paxos_group_id, instance_id, checkpoint, sync);

    proto::LockPaxosArgs args;
    bool succ{args.ParseFromString(paxos_value)};
    if (!succ) {
        QLErr("topic_id %d paxos_group_id %d instance_id %llu ParseFromString err",
              impl_->lock->GetTopicID(), paxos_group_id, instance_id);

        return false;
    }

    return DoExecute(impl_->lock, LockDb::Type::LEVELDB,
                     paxos_group_id, instance_id, args, sync);
}

const uint64_t LockSM::GetCheckpointInstanceID(const int paxos_group_id) const {
    comm::LockSMBP::GetThreadInstance()->
            OnGetCheckpointInstanceID(impl_->lock->GetTopicID(), paxos_group_id);

    const uint64_t checkpoint{impl_->lock->GetLockMgr()->checkpoint(paxos_group_id)};
    QLVerb("topic_id %d paxos_group_id %d cp %llu",
           impl_->lock->GetTopicID(), paxos_group_id, checkpoint);

    return checkpoint;
}

int LockSM::GetCheckpointState(const int paxos_group_id, string &dir_path,
                               vector<string> &file_list) {
    comm::LockSMBP::GetThreadInstance()->
            OnGetCheckpointState(impl_->lock->GetTopicID(), paxos_group_id);
    QLInfo("topic_id %d paxos_group_id %d dir \"%s\"",
           impl_->lock->GetTopicID(), paxos_group_id, dir_path.c_str());

    // return mirror
    dir_path = impl_->mirror_dir_path + to_string(paxos_group_id);
    file_list.clear();

    int ret{comm::utils::RecursiveListDir(dir_path, &file_list, nullptr, true)};
    if (0 != ret) {
        QLErr("topic_id %d paxos_group_id %d RecursiveListDir err %d",
              impl_->lock->GetTopicID(), paxos_group_id, ret);

        return -1;
    }

    for (auto &&file_path : file_list) {
        QLInfo("topic_id %d paxos_group_id %d file \"%s\"",
               impl_->lock->GetTopicID(), paxos_group_id, file_path.c_str());
    }

    return 0;
}

int LockSM::LoadCheckpointState(const int paxos_group_id,
                                const string &checkpoint_tmp_file_dir_path,
                                const vector<string> &file_list,
                                const uint64_t checkpoint_instance_id) {
    comm::LockSMBP::GetThreadInstance()->
            OnLoadCheckpointState(impl_->lock->GetTopicID(), paxos_group_id);
    QLInfo("topic_id %d paxos_group_id %d dir \"%s\" cp_instance_id %llu",
           impl_->lock->GetTopicID(), paxos_group_id,
           checkpoint_tmp_file_dir_path.c_str(), checkpoint_instance_id);

    // 1. check
    if (checkpoint_tmp_file_dir_path.empty() || 0 >= file_list.size()) {
        QLErr("topic_id %d paxos_group_id %d dir %s cp_instance_id %llu file_list empty",
              impl_->lock->GetTopicID(), paxos_group_id,
              checkpoint_tmp_file_dir_path.c_str(), checkpoint_instance_id);

        return -1;
    }

    impl_->lock->GetLockMgr()->leveldb(paxos_group_id).Dispose();

    string mirror_path{impl_->mirror_dir_path + to_string(paxos_group_id)};

    // 2. clear mirror
    int ret{comm::utils::RecursiveRemoveDir(mirror_path, true, false)};
    if (0 != ret) {
        QLErr("topic_id %d paxos_group_id %d RecursiveRemoveDir \"%s\" err %d",
              impl_->lock->GetTopicID(), paxos_group_id, mirror_path.c_str(), ret);

        return -1;
    }

    // 3. copy files from tmp to mirror
    ret = comm::utils::RecursiveCopyDir(checkpoint_tmp_file_dir_path, mirror_path, true, false);
    if (0 != ret) {
        QLErr("topic_id %d paxos_group_id %d RecursiveCopyDir \"%s\" -> \"%s\" err %d",
              impl_->lock->GetTopicID(), paxos_group_id,
              checkpoint_tmp_file_dir_path.c_str(), mirror_path.c_str(), ret);

        return -1;
    }

    //for (auto &&file_path : file_list) {
    //    int sub_ret{CopyFile(checkpoint_tmp_file_dir_path + "/" +
    //                         file_path, mirror_path + "/" + file_path)};
    //    CLogImpt("topic_id %d paxos_group_id %d file \"%s\"",
    //             impl_->lock->GetTopicID(), paxos_group_id, file_path.c_str());
    //    if (0 != sub_ret) {
    //        CLogErr("topic_id %d paxos_group_id %d CopyFile \"%s\" err %d",
    //                impl_->lock->GetTopicID(), paxos_group_id, file_path.c_str(), ret);

    //        return -1;
    //    }
    //}

    // no need to recover map and leveldb, paxos will restart myself immediately

    constexpr uint32_t sleep_s{10u};
    QLInfo("topic_id %d paxos_group_id %d begin sleep %u",
           impl_->lock->GetTopicID(), paxos_group_id, sleep_s);
    sleep(sleep_s);
    QLInfo("topic_id %d paxos_group_id %d end sleep %u",
           impl_->lock->GetTopicID(), paxos_group_id, sleep_s);

    return ret;
}


}  // namespace lock

}  // namespace phxqueue


//gzrd_Lib_CPP_Version_ID--start
#ifndef GZRD_SVN_ATTR
#define GZRD_SVN_ATTR "0"
#endif
static char gzrd_Lib_CPP_Version_ID[] __attribute__((used))="$HeadURL: http://scm-gy.tencent.com/gzrd/gzrd_mail_rep/phoenix_proj/trunk/phxqueue/src/lock/locksm.cpp $ $Id: locksm.cpp 2124396 2017-06-15 03:58:06Z walnuthe $ " GZRD_SVN_ATTR "__file__";
// gzrd_Lib_CPP_Version_ID--end

