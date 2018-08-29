/*
Tencent is pleased to support the open source community by making PhxQueue available.
Copyright (C) 2017 THL A29 Limited, a Tencent company. All rights reserved.
Licensed under the BSD 3-Clause License (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at

<https://opensource.org/licenses/BSD-3-Clause>

Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.
*/



#include "phxqueue/lock/lock.h"

#include <cinttypes>
#include <string>

#include "phxpaxos/node.h"

#include "phxqueue/comm.h"
#include "phxqueue/config.h"

#include "phxqueue/lock/lock.h"
#include "phxqueue/lock/cleanthread.h"
#include "phxqueue/lock/keepmasterthread.h"
#include "phxqueue/lock/lockdb.h"
#include "phxqueue/lock/lockmgr.h"
#include "phxqueue/lock/locksm.h"
#include "phxqueue/lock/lockutils.h"


namespace {


inline uint32_t HashUi32(const std::string &s) {
    return std::hash<std::string>()(s) >> ((sizeof(std::size_t) - sizeof(uint32_t)) * 8);
}


}


namespace phxqueue {

namespace lock {


using namespace phxpaxos;
using namespace std;


class Lock::LockImpl {
  public:
    LockImpl() = default;
    virtual ~LockImpl() = default;

    LockOption opt;
    int topic_id{-1};
    unique_ptr<Node> node;
    vector<unique_ptr<LockSM>> sms;
    unique_ptr<LockMgr> lock_mgr;
    unique_ptr<CleanThread> clean_thread;
    unique_ptr<KeepMasterThread> keep_master_thread;

    comm::proto::Addr addr;
};

Lock::Lock(const LockOption &opt) : impl_(new LockImpl()) {
    assert(impl_);
    impl_->opt = opt;
}

Lock::~Lock() {}

const LockOption *Lock::GetLockOption() const {
    return &impl_->opt;
}

comm::RetCode Lock::Init() {
    if (impl_->opt.log_func) {
        comm::Logger::GetInstance()->SetLogFunc(impl_->opt.log_func);
    }

    if (impl_->opt.config_factory_create_func) {
        plugin::ConfigFactory::SetConfigFactoryCreateFunc(impl_->opt.config_factory_create_func);
    }

    if (impl_->opt.break_point_factory_create_func) {
        plugin::BreakPointFactory::SetBreakPointFactoryCreateFunc(
                impl_->opt.break_point_factory_create_func);
    }

    impl_->addr.set_ip(impl_->opt.ip);
    impl_->addr.set_port(impl_->opt.port);
    impl_->addr.set_paxos_port(impl_->opt.paxos_port);


    comm::RetCode ret{InitTopicID()};
    if (comm::RetCode::RET_OK != ret) {
        QLErr("InitTopicID ret %d", as_integer(ret));

        return ret;
    }

    comm::LockBP::GetThreadInstance()->OnInit(impl_->topic_id);

    // init data path
    auto mirror_dir_path(impl_->opt.data_dir_path + "/mirror/");
    if (!comm::utils::CreateDir(mirror_dir_path)) {
        QLErr("mirror_dir_path %s not exist", mirror_dir_path.c_str());
        return comm::RetCode::RET_DIR_NOT_EXIST;
    }

    impl_->lock_mgr = unique_ptr<LockMgr>(new LockMgr(this));
    if (comm::RetCode::RET_OK != (ret = impl_->lock_mgr->Init(mirror_dir_path))) {
        QLErr("lock_mgr Init ret %d", as_integer(ret));

        return ret;
    }

    impl_->clean_thread = unique_ptr<CleanThread>(new CleanThread(this));
    impl_->keep_master_thread = unique_ptr<KeepMasterThread>(new KeepMasterThread(this));

    ret = PaxosInit(mirror_dir_path);
    if (comm::RetCode::RET_OK != ret) {
        QLErr("PaxosInit ret %d", as_integer(ret));

        return ret;
    }

    impl_->clean_thread->Run();
    impl_->keep_master_thread->Run();

    return comm::RetCode::RET_OK;
}

comm::RetCode Lock::Dispose() {
    comm::LockBP::GetThreadInstance()->OnDispose(impl_->topic_id);

    if (impl_->keep_master_thread) impl_->keep_master_thread->Stop();
    if (impl_->clean_thread) impl_->clean_thread->Stop();

    return impl_->lock_mgr->Dispose();
}

int Lock::GetTopicID() {
    return impl_->topic_id;
}

const comm::proto::Addr &Lock::GetAddr() {
    return impl_->addr;
}

Node *Lock::GetNode() {
    return impl_->node.get();
}

LockMgr *Lock::GetLockMgr() {
    return impl_->lock_mgr.get();
}

comm::RetCode Lock::GetString(const comm::proto::GetStringRequest &req,
                              comm::proto::GetStringResponse &resp) {
    comm::LockBP::GetThreadInstance()->OnGetString(req);

    if (0 >= req.key().size()) {
        QLErr("key \"%s\" invalid", req.key().c_str());

        return comm::RetCode::RET_ERR_KEY;
    }

    uint32_t paxos_group_id{HashUi32(req.key()) % impl_->opt.nr_group};

    comm::RetCode ret{CheckMaster(paxos_group_id, *resp.mutable_redirect_addr())};
    if (comm::RetCode::RET_OK != ret) {
        comm::LockBP::GetThreadInstance()->OnGetStringRequestInvalid(req);
        QLErr("paxos_group %d key \"%s\" CheckMaster err %d",
              paxos_group_id, req.key().c_str(), ret);

        return ret;
    }
    comm::LockBP::GetThreadInstance()->OnGetStringCheckMasterPass(req);
    QLVerb("paxos_group %d key \"%s\" node %" PRIu64 " master 1",
           paxos_group_id, req.key().c_str(), impl_->node->GetMyNodeID());

    proto::LocalRecordInfo local_record_info;

    // begin mutex
    {

        // prevent paxos from writing
        comm::utils::MutexGuard guard(impl_->lock_mgr->map(paxos_group_id).mutex());
        ret = impl_->lock_mgr->map(paxos_group_id).GetString(req.key(), local_record_info);

    }
    // end mutex

    if (comm::RetCode::RET_ERR_KEY_NOT_EXIST == ret) {
        QLVerb("paxos_group %d key \"%s\" GetString not exist", paxos_group_id, req.key().c_str());

        return ret;
    } else if (comm::RetCode::RET_OK != ret) {
        QLErr("paxos_group %d key \"%s\" GetString err %d", paxos_group_id, req.key().c_str(), ret);

        return ret;
    }

    const auto &string_info(resp.mutable_string_info());
    string_info->set_key(req.key());
    string_info->set_version(local_record_info.version());
    string_info->set_value(local_record_info.value());
    string_info->set_lease_time_ms(local_record_info.lease_time_ms());
    QLInfo("paxos_group %d key \"%s\" GetString ok resp.ver %llu "
           "resp.lease_time_ms %llu resp.expire_time_ms %llu",
           paxos_group_id, req.key().c_str(), local_record_info.version(),
           local_record_info.lease_time_ms(), local_record_info.expire_time_ms());

    return ret;
}

comm::RetCode Lock::SetString(const comm::proto::SetStringRequest &req,
                              comm::proto::SetStringResponse &resp) {
    comm::LockBP::GetThreadInstance()->OnSetString(req);

    auto &&string_info(req.string_info());

    if (0 >= string_info.key().size()) {
        QLErr("key \"%s\" invalid", string_info.key().c_str());

        return comm::RetCode::RET_ERR_KEY;
    }

    uint32_t paxos_group_id{HashUi32(string_info.key()) % impl_->opt.nr_group};

    comm::RetCode ret{CheckMaster(paxos_group_id, *resp.mutable_redirect_addr())};
    if (comm::RetCode::RET_OK != ret) {
        comm::LockBP::GetThreadInstance()->OnSetStringRequestInvalid(req);
        QLErr("paxos_group %d key \"%s\" CheckMaster err %d req.value \"%s\"",
              paxos_group_id, string_info.key().c_str(), ret, string_info.value().c_str());

        return ret;
    }
    comm::LockBP::GetThreadInstance()->OnSetStringCheckMasterPass(req);
    QLVerb("paxos_group %d key \"%s\" node %" PRIu64
           " master 1 req.ver %llu req.value \"%s\" lease_time_ms %llu",
           paxos_group_id, string_info.key().c_str(), impl_->node->GetMyNodeID(),
           string_info.version(), string_info.value().c_str(), string_info.lease_time_ms());

    return PaxosSetString(req, resp);
}

comm::RetCode Lock::DeleteString(const comm::proto::DeleteStringRequest &req,
                                 comm::proto::DeleteStringResponse &resp) {
    comm::LockBP::GetThreadInstance()->OnDeleteString(req);

    auto &&string_key_info(req.string_key_info());

    if (0 >= string_key_info.key().size()) {
        QLErr("key \"%s\" invalid", string_key_info.key().c_str());

        return comm::RetCode::RET_ERR_KEY;
    }

    uint32_t paxos_group_id{HashUi32(string_key_info.key()) % impl_->opt.nr_group};

    comm::RetCode ret{CheckMaster(paxos_group_id, *resp.mutable_redirect_addr())};
    if (comm::RetCode::RET_OK != ret) {
        comm::LockBP::GetThreadInstance()->OnDeleteStringRequestInvalid(req);
        QLErr("paxos_group %d key \"%s\" CheckMaster err %d",
              paxos_group_id, string_key_info.key().c_str(), ret);

        return ret;
    }
    comm::LockBP::GetThreadInstance()->OnDeleteStringCheckMasterPass(req);
    QLVerb("paxos_group %d key \"%s\" node %" PRIu64 " master 1 req.ver %llu",
           paxos_group_id, string_key_info.key().c_str(), impl_->node->GetMyNodeID(),
           string_key_info.version());

    return PaxosDeleteString(req, resp);
}

comm::RetCode Lock::GetLockInfo(const comm::proto::GetLockInfoRequest &req,
                                comm::proto::GetLockInfoResponse &resp) {
    comm::LockBP::GetThreadInstance()->OnGetLockInfo(req);

    if (0 >= req.lock_key().size()) {
        comm::LockBP::GetThreadInstance()->OnGetLockInfoRequestInvalid(req);
        QLErr("lock \"%s\" invalid", req.lock_key().c_str());

        return comm::RetCode::RET_ERR_KEY;
    }

    uint32_t paxos_group_id{HashUi32(req.lock_key()) % impl_->opt.nr_group};

    comm::RetCode ret{CheckMaster(paxos_group_id, *resp.mutable_redirect_addr())};
    if (comm::RetCode::RET_OK != ret) {
        QLErr("paxos_group %d lock \"%s\" CheckMaster err %d",
              paxos_group_id, req.lock_key().c_str(), ret);
        // TODO:
        //OssAttrInc(impl_->opt.oss_attr_id, 23u, 1u);

        return ret;
    }
    comm::LockBP::GetThreadInstance()->OnGetLockInfoCheckMasterPass(req);
    QLVerb("paxos_group %d lock \"%s\" node %" PRIu64 " master 1",
           paxos_group_id, req.lock_key().c_str(), impl_->node->GetMyNodeID());

    proto::LocalRecordInfo local_record_info;

    // begin mutex
    {

        // prevent paxos from writing
        comm::utils::MutexGuard guard(impl_->lock_mgr->map(paxos_group_id).mutex());
        ret = impl_->lock_mgr->map(paxos_group_id).GetLock(req.lock_key(), local_record_info);

    }
    // end mutex

    if (comm::RetCode::RET_ERR_KEY_NOT_EXIST == ret) {
        QLVerb("paxos_group %d lock \"%s\" GetLock not exist",
               paxos_group_id, req.lock_key().c_str());
        // TODO:
        //OssAttrInc(impl_->opt.oss_attr_id, 25u, 1u);

        return ret;
    } else if (comm::RetCode::RET_OK != ret) {
        QLErr("paxos_group %d lock \"%s\" GetLock err %d",
              paxos_group_id, req.lock_key().c_str(), ret);
        // TODO:
        //OssAttrInc(impl_->opt.oss_attr_id, 24u, 1u);

        return ret;
    }

    LocalRecordInfo2LockInfo(local_record_info, *resp.mutable_lock_info());
    QLInfo("paxos_group %d lock \"%s\" GetLock ok resp.ver %llu resp.client_id \"%s\" "
           "resp.lease_time_ms %llu resp.expire_time_ms %llu",
           paxos_group_id, req.lock_key().c_str(), resp.lock_info().version(),
           resp.lock_info().client_id().c_str(), resp.lock_info().lease_time_ms(),
           local_record_info.expire_time_ms());

    return ret;
}

// ret: RET_OK if acquired, others if not acquired
comm::RetCode Lock::AcquireLock(const comm::proto::AcquireLockRequest &req,
                                comm::proto::AcquireLockResponse &resp) {
    comm::LockBP::GetThreadInstance()->OnAcquireLock(req);

    auto &&lock_info(req.lock_info());

    if (0 >= lock_info.lock_key().size()) {
        QLErr("lock \"%s\" invalid", lock_info.lock_key().c_str());

        return comm::RetCode::RET_ERR_KEY;
    }

    uint32_t paxos_group_id{HashUi32(lock_info.lock_key()) % impl_->opt.nr_group};

    comm::RetCode ret{CheckMaster(paxos_group_id, *resp.mutable_redirect_addr())};
    if (comm::RetCode::RET_OK != ret) {
        comm::LockBP::GetThreadInstance()->OnAcquireLockRequestInvalid(req);
        QLErr("paxos_group %d lock \"%s\" CheckMaster err %d req.client_id \"%s\"",
              paxos_group_id, lock_info.lock_key().c_str(), ret, lock_info.client_id().c_str());

        return ret;
    }
    comm::LockBP::GetThreadInstance()->OnAcquireLockCheckMasterPass(req);
    QLVerb("paxos_group %d lock \"%s\" node %" PRIu64
           " master 1 req.ver %llu req.client_id \"%s\" req.lease_time_ms %llu",
           paxos_group_id, lock_info.lock_key().c_str(), impl_->node->GetMyNodeID(),
           lock_info.version(), lock_info.client_id().c_str(), lock_info.lease_time_ms());

    // compare version
    proto::LocalRecordInfo local_record_info;

    // begin mutex
    {

        // ensure clean thread not cleaning
        comm::utils::MutexGuard guard(impl_->lock_mgr->map(paxos_group_id).mutex());
        ret = impl_->lock_mgr->map(paxos_group_id).GetLock(lock_info.lock_key(), local_record_info);

    }
    // end mutex

    if (comm::RetCode::RET_ERR_KEY_NOT_EXIST == ret) {
        QLVerb("paxos_group %d lock \"%s\" GetLock not exist",
               paxos_group_id, lock_info.lock_key().c_str());
        // TODO:
        //OssAttrInc(impl_->opt.oss_attr_id, 15u, 1u);
    } else if (comm::RetCode::RET_OK != ret) {
        QLErr("paxos_group %d lock \"%s\" GetLock err %d",
              paxos_group_id, lock_info.lock_key().c_str(), ret);
        // TODO:
        //OssAttrInc(impl_->opt.oss_attr_id, 14u, 1u);

        return ret;
    }

    if (comm::RetCode::RET_ERR_KEY_NOT_EXIST != ret &&
        local_record_info.version() != lock_info.version()) {
        QLErr("paxos_group %d lock \"%s\" map.ver %llu != req.ver %llu req.client_id \"%s\"",
              paxos_group_id, lock_info.lock_key().c_str(), local_record_info.version(),
              lock_info.version(), lock_info.client_id().c_str());
        // TODO:
        //OssAttrInc(impl_->opt.oss_attr_id, 16u, 1u);

        return comm::RetCode::RET_ERR_VERSION_NOT_EQUAL;
    }

    uint64_t now{comm::utils::Time::GetSteadyClockMS()};
    if (local_record_info.value() != lock_info.client_id() &&
        now < local_record_info.expire_time_ms()) {
        // warning only
        // if NOT occur in changing master, client has bug
        QLErr("paxos_group %d lock \"%s\" (map.client_id \"%s\" != req.client_id \"%s\") && (now %"
              PRIu64 " < expire_time_ms %llu)",
              paxos_group_id, lock_info.lock_key().c_str(), local_record_info.value().c_str(),
              lock_info.client_id().c_str(), now, local_record_info.expire_time_ms());
        // TODO:
        //OssAttrInc(impl_->opt.oss_attr_id, 17u, 1u);
    }

    ret = PaxosAcquireLock(req, resp);

    if (0 >= lock_info.lease_time_ms()) {

        // ensure clean thread not cleaning
        comm::utils::MutexGuard guard(impl_->lock_mgr->map(paxos_group_id).mutex());
        ret = impl_->lock_mgr->map(paxos_group_id).DeleteLock(lock_info.lock_key(), false);
        QLVerb("paxos_group %d lock \"%s\" DeleteLock",
               paxos_group_id, lock_info.lock_key().c_str());

    }

    return ret;
}

comm::RetCode Lock::PaxosInit(const string &mirror_dir_path) {
    comm::RetCode ret{comm::RetCode::RET_ERR_UNINIT};

    phxpaxos::Options opts;

    // 1. set members

    const auto &topic_id(impl_->topic_id);

    shared_ptr<const config::TopicConfig> topic_config;
    if (comm::RetCode::RET_OK != (ret = config::GlobalConfig::GetThreadInstance()->
                                  GetTopicConfigByTopicID(topic_id, topic_config))) {
        QLErr("GetTopicConfig ret %d topic_id %d", as_integer(ret), topic_id);

        return ret;
    }

    shared_ptr<const config::LockConfig> lock_config;
    if (comm::RetCode::RET_OK != (ret = config::GlobalConfig::GetThreadInstance()->
                                  GetLockConfig(topic_id, lock_config))) {
        QLErr("GetLockConfig ret %d topic_id %d", as_integer(ret), topic_id);

        return ret;
    }

    shared_ptr<const config::proto::Lock> lock;
    if (comm::RetCode::RET_OK != (ret = lock_config->GetLockByAddr(impl_->addr, lock))) {
        QLErr("GetLockByAddr ret %d", as_integer(ret));

        return ret;
    }

    for (size_t i{0}; i < lock->addrs_size(); ++i) {
        auto &&addr(lock->addrs(i));
        opts.vecNodeInfoList.emplace_back(addr.ip(), addr.paxos_port());
    }


    // 2. set nr_group
    opts.iGroupCount = impl_->opt.nr_group;
    for (int i{0}; i < opts.iGroupCount; ++i) {
        phxpaxos::GroupSMInfo sm_info;
        sm_info.iGroupIdx = i;
        unique_ptr<LockSM> sm(new LockSM(this, mirror_dir_path));
        sm_info.vecSMList.push_back(sm.get());
        sm_info.bIsUseMaster = true;
        opts.vecGroupSMInfoList.push_back(sm_info);

        impl_->sms.push_back(move(sm));
    }


    // 3.0. init data path
    auto nodedb_dir_path(impl_->opt.data_dir_path + "/nodedb/");
    if (!comm::utils::CreateDir(nodedb_dir_path)) {
        QLErr("nodedb_dir_path %s not exist", nodedb_dir_path.c_str());
        return comm::RetCode::RET_DIR_NOT_EXIST;
    }

    // 3.1. other
    opts.oMyNode.SetIPPort(impl_->opt.ip, impl_->opt.paxos_port);
    opts.bUseMembership = false;
    opts.sLogStoragePath = nodedb_dir_path;
    opts.pLogFunc = comm::LogFuncForPhxPaxos;

    opts.bUseCheckpointReplayer = true;
    opts.iSyncInterval = topic_config->GetProto().topic().lock_paxos_fsync_interval();
    opts.bUseBatchPropose = false;
    opts.iIOThreadCount = impl_->opt.nr_paxos_io_thread;


    // 4. other init on opts
    BeforeRunNode(opts);


    // 5. run
    Node *node{nullptr};
    int paxos_ret{Node::RunNode(opts, node)};
    if (0 != paxos_ret) {
        QLErr("RunNode paxos_ret %d", paxos_ret);

        return comm::RetCode::RET_ERR_PAXOS_RUN_NODE;
    }
    impl_->node.reset(node);
    node->ContinueCheckpointReplayer();
    node->ContinuePaxosLogCleaner();

    return comm::RetCode::RET_OK;
}

comm::RetCode Lock::CheckMaster(const int paxos_group_id, comm::proto::Addr &redirect_addr) {
    comm::RetCode ret;

    if (impl_->node->IsIMMaster(paxos_group_id))
        return comm::RetCode::RET_OK;

    auto &&node_info = impl_->node->GetMaster(paxos_group_id);
    if (node_info.GetIP().empty() || 0 == node_info.GetPort() || "0.0.0.0" == node_info.GetIP())
        return comm::RetCode::RET_ERR_NO_MASTER;

    const auto &topic_id(impl_->topic_id);

    shared_ptr<const config::LockConfig> lock_config;
    if (comm::RetCode::RET_OK != (ret = config::GlobalConfig::GetThreadInstance()->
                                  GetLockConfig(topic_id, lock_config))) {
        QLErr("GetLockConfig ret %d topic_id %d", as_integer(ret), topic_id);

        return ret;
    }

    shared_ptr<const config::proto::Lock> lock;
    if (comm::RetCode::RET_OK != (ret = lock_config->GetLockByAddr(impl_->addr, lock))) {
        QLErr("GetLockByAddr ret %d", as_integer(ret));

        return ret;
    }

    for (size_t i{0}; lock->addrs_size() > i; ++i) {
        auto &&tmp_addr(lock->addrs(i));
        if (tmp_addr.ip() == node_info.GetIP() && tmp_addr.paxos_port() == node_info.GetPort()) {
            redirect_addr = tmp_addr;

            return comm::RetCode::RET_ERR_NOT_MASTER;
        }
    }

    return comm::RetCode::RET_ERR_RANGE_ADDR;
}

// ret: RET_OK if set, others if not set
comm::RetCode Lock::PaxosSetString(const comm::proto::SetStringRequest &req,
                                   comm::proto::SetStringResponse &resp) {
    comm::LockBP::GetThreadInstance()->OnPaxosSetString(req);

    auto &&string_info(req.string_info());
    uint32_t paxos_group_id{HashUi32(string_info.key()) % impl_->opt.nr_group};

    // paxos
    proto::LockPaxosArgs args;

    // 1. make args
    *args.mutable_set_string_req() = req;

    // 2. serialize args to paxos value
    string buf;
    args.SerializeToString(&buf);

    // 3. send to paxos
    comm::LockBP::GetThreadInstance()->OnSetStringPropose(req);

    LockContext lc;
    phxpaxos::SMCtx sm_ctx(LockSM::ID, &lc);
    uint64_t instance_id{0};

    uint64_t t1{comm::utils::Time::GetSteadyClockMS()};
    int paxos_ret{impl_->node->Propose(paxos_group_id, buf, instance_id, &sm_ctx)};
    uint64_t t2{comm::utils::Time::GetSteadyClockMS()};
    uint64_t used_time_ms{t2 - t1};

    if (phxpaxos::PaxosTryCommitRet_OK != paxos_ret) {
        comm::LockBP::GetThreadInstance()->OnSetStringProposeErr(req, used_time_ms);
        QLErr("paxos_group %d key \"%s\" Propose err %d buf.size %zu",
              paxos_group_id, string_info.key().c_str(),
              paxos_ret, buf.size());
        switch (paxos_ret) {
            case phxpaxos::PaxosTryCommitRet_Timeout:
                comm::LockBP::GetThreadInstance()->OnSetStringProposeErrTimeout(req);
                return comm::RetCode::RET_ERR_PROPOSE_TIMEOUT;
            case phxpaxos::PaxosTryCommitRet_TooManyThreadWaiting_Reject:
                comm::LockBP::GetThreadInstance()->OnSetStringProposeErrTooManyThreadWaitingReject(req);
                return comm::RetCode::RET_ERR_PROPOSE_FAST_REJECT;
            case phxpaxos::PaxosTryCommitRet_Value_Size_TooLarge:
                comm::LockBP::GetThreadInstance()->OnSetStringProposeErrValueSizeTooLarge(req);
                return comm::RetCode::RET_ERR_SIZE_TOO_LARGE;
            default:
                comm::LockBP::GetThreadInstance()->OnSetStringProposeErrOther(req);
                return comm::RetCode::RET_ERR_PROPOSE;
        };
    }

    if (comm::RetCode::RET_OK != lc.result) {
        QLErr("paxos_group %d key \"%s\" Propose err %d instance_id %" PRIu64
              " buf.size %zu", paxos_group_id,
              string_info.key().c_str(), lc.result, instance_id, buf.size());
        comm::LockBP::GetThreadInstance()->OnSetStringProposeErrResult(req, instance_id, used_time_ms);

        return lc.result;
    }
    comm::LockBP::GetThreadInstance()->OnSetStringProposeSucc(req, instance_id, used_time_ms);
    QLInfo("paxos_group %d key \"%s\" Propose ok instance_id %" PRIu64
           " buf.size %zu", paxos_group_id,
           string_info.key().c_str(), instance_id, buf.size());

    return comm::RetCode::RET_OK;
}

// ret: RET_OK if set, others if not set
comm::RetCode Lock::PaxosDeleteString(const comm::proto::DeleteStringRequest &req,
                                      comm::proto::DeleteStringResponse &resp) {
    comm::proto::SetStringRequest set_string_req;
    comm::proto::SetStringResponse set_string_resp;

    set_string_req.set_topic_id(req.topic_id());
    set_string_req.set_lock_id(req.lock_id());
    set_string_req.mutable_string_info()->set_key(req.string_key_info().key());
    set_string_req.mutable_string_info()->set_version(req.string_key_info().version());
    set_string_req.mutable_master_addr()->CopyFrom(req.master_addr());

    comm::RetCode ret{PaxosSetString(set_string_req, set_string_resp)};

    resp.mutable_redirect_addr()->CopyFrom(set_string_resp.redirect_addr());

    return ret;
}

// ret: RET_OK if acquired, others if not acquired
comm::RetCode Lock::PaxosAcquireLock(const comm::proto::AcquireLockRequest &req,
                                     comm::proto::AcquireLockResponse &resp) {
    comm::LockBP::GetThreadInstance()->OnPaxosAcquireLock(req);

    auto &&lock_info(req.lock_info());
    uint32_t paxos_group_id{HashUi32(lock_info.lock_key()) % impl_->opt.nr_group};

    // paxos
    proto::LockPaxosArgs args;

    // 1. make args
    *args.mutable_acquire_lock_req() = req;

    // 2. serialize args to paxos value
    string buf;
    args.SerializeToString(&buf);

    // 3. send to paxos
    comm::LockBP::GetThreadInstance()->OnAcquireLockPropose(req);

    LockContext lc;
    phxpaxos::SMCtx sm_ctx(LockSM::ID, &lc);
    uint64_t instance_id{0};

    uint64_t t1{comm::utils::Time::GetSteadyClockMS()};
    int paxos_ret{impl_->node->Propose(paxos_group_id, buf, instance_id, &sm_ctx)};
    uint64_t t2{comm::utils::Time::GetSteadyClockMS()};
    uint64_t used_time_ms{t2 - t1};

    if (phxpaxos::PaxosTryCommitRet_OK != paxos_ret) {
        comm::LockBP::GetThreadInstance()->OnAcquireLockProposeErr(req, used_time_ms);
        QLErr("paxos_group %d lock \"%s\" Propose err %d buf.size %zu req.client_id \"%s\"",
              paxos_group_id, lock_info.lock_key().c_str(),
              paxos_ret, buf.size(), lock_info.client_id().c_str());
        switch (paxos_ret) {
            case phxpaxos::PaxosTryCommitRet_Timeout:
                comm::LockBP::GetThreadInstance()->OnAcquireLockProposeErrTimeout(req);
                return comm::RetCode::RET_ERR_PROPOSE_TIMEOUT;
            case phxpaxos::PaxosTryCommitRet_TooManyThreadWaiting_Reject:
                comm::LockBP::GetThreadInstance()->OnAcquireLockProposeErrTooManyThreadWaitingReject(req);
                return comm::RetCode::RET_ERR_PROPOSE_FAST_REJECT;
            case phxpaxos::PaxosTryCommitRet_Value_Size_TooLarge:
                comm::LockBP::GetThreadInstance()->OnAcquireLockProposeErrValueSizeTooLarge(req);
                return comm::RetCode::RET_ERR_SIZE_TOO_LARGE;
            default:
                comm::LockBP::GetThreadInstance()->OnAcquireLockProposeErrOther(req);
                return comm::RetCode::RET_ERR_PROPOSE;
        };
    }

    if (comm::RetCode::RET_OK != lc.result) {
        QLErr("paxos_group %d lock \"%s\" Propose err %d instance_id %" PRIu64
              " buf.size %zu req.client_id \"%s\"", paxos_group_id,
              lock_info.lock_key().c_str(), lc.result, instance_id, buf.size(),
              lock_info.client_id().c_str());
        comm::LockBP::GetThreadInstance()->OnAcquireLockProposeErrResult(req, instance_id, used_time_ms);

        return lc.result;
    }
    comm::LockBP::GetThreadInstance()->OnAcquireLockProposeSucc(req, instance_id, used_time_ms);
    QLInfo("paxos_group %d lock \"%s\" Propose ok instance_id %" PRIu64
           " buf.size %zu req.client_id \"%s\"", paxos_group_id,
           lock_info.lock_key().c_str(), instance_id, buf.size(),
           lock_info.client_id().c_str());

    return comm::RetCode::RET_OK;
}

bool Lock::NeedDropMaster(const int paxos_group_id) {
    return false;
}

comm::RetCode Lock::InitTopicID() {

    if (!impl_->opt.topic.empty()) {
        comm::RetCode ret{config::GlobalConfig::GetThreadInstance()->
                GetTopicIDByTopicName(impl_->opt.topic, impl_->topic_id)};
        if (comm::RetCode::RET_OK != ret) {
            QLErr("GetTopicIDByTopicName ret %d topic %s",
                  as_integer(ret), impl_->opt.topic.c_str());
        }

        return ret;
    }

    QLInfo("find topic by addr %s:%d:%d", impl_->addr.ip().c_str(),
           impl_->addr.port(), impl_->addr.paxos_port());

    set<int> topic_ids;
    comm::RetCode ret{config::GlobalConfig::GetThreadInstance()->GetAllTopicID(topic_ids)};
    if (comm::RetCode::RET_OK != ret) {
        QLErr("GetAllTopicID ret %d", comm::as_integer(ret));

        return ret;
    }

    QLInfo("topic_ids size %zu", topic_ids.size());

    for (auto &&topic_id : topic_ids) {
        QLInfo("check topic %d", topic_id);

        shared_ptr<const config::LockConfig> lock_config;
        comm::RetCode topic_ret{config::GlobalConfig::GetThreadInstance()->
                GetLockConfig(topic_id, lock_config)};
        if (comm::RetCode::RET_OK != topic_ret) {
            QLErr("GetLockConfig ret %d", comm::as_integer(topic_ret));

            continue;
        }

        int lock_id{-1};
        topic_ret = lock_config->GetLockIDByAddr(impl_->addr, lock_id);
        if (comm::RetCode::RET_OK == topic_ret) {
            QLInfo("found toipc %d addr %s:%d", topic_id,
                   impl_->addr.ip().c_str(), impl_->addr.port());
            impl_->topic_id = topic_id;

            return comm::RetCode::RET_OK;
        }
    }

    return comm::RetCode::RET_ERR_RANGE_ADDR;
}


}  // namespace lock

}  // namespace phxqueue

