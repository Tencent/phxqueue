/*
Tencent is pleased to support the open source community by making PhxQueue available.
Copyright (C) 2017 THL A29 Limited, a Tencent company. All rights reserved.
Licensed under the BSD 3-Clause License (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at

<https://opensource.org/licenses/BSD-3-Clause>

Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.
*/



#include "phxqueue/store/store.h"

#include <cinttypes>

#include "phxqueue/comm.h"
#include "phxqueue/config.h"
#include "phxpaxos/node.h"
#include "phxpaxos_plugin/monitor.h"

#include "phxqueue/store/basemgr.h"
#include "phxqueue/store/syncctrl.h"
#include "phxqueue/store/keepsyncthread.h"
#include "phxqueue/store/keepmasterthread.h"
#include "phxqueue/store/storesm.h"
#include "phxqueue/store/storemeta.h"


namespace phxqueue {

namespace store {


using namespace std;
using namespace phxpaxos;


class Store::StoreImpl {
  public:
    StoreImpl() = default;
    virtual ~StoreImpl() = default;

    int topic_id;
    StoreOption opt;
    unique_ptr<Node> node;
    vector<unique_ptr<StoreSM>> sms;
    unique_ptr<BaseMgr> basemgr;
    unique_ptr<SyncCtrl> sync;
    unique_ptr<CheckPointStatMgr> cpmgr;
    unique_ptr<KeepSyncThread> keep_sync_thread;
    unique_ptr<KeepMasterThread> keep_master_thread;

    comm::proto::Addr addr;
};

Store::Store(const StoreOption &opt) : impl_(new StoreImpl()) {
    assert(impl_);
    impl_->opt = opt;
}

Store::~Store() {
    if (impl_->keep_sync_thread) impl_->keep_sync_thread->Stop();
    if (impl_->keep_master_thread) impl_->keep_master_thread->Stop();
}

const StoreOption *Store::GetStoreOption() const {
    return &impl_->opt;
}

comm::RetCode Store::InitTopicID() {
    comm::RetCode ret;

    if (!impl_->opt.topic.empty()) {
        if (comm::RetCode::RET_OK !=
            (ret = config::GlobalConfig::GetThreadInstance()->
             GetTopicIDByTopicName(impl_->opt.topic, impl_->topic_id))) {
            QLErr("GetTopicIDByTopicName ret %d topic %s",
                  as_integer(ret), impl_->opt.topic.c_str());
        }
        return ret;
    }

    QLInfo("no topic name. find toipc_id by addr(%s:%d)",
           impl_->addr.ip().c_str(), impl_->addr.port());

    set<int> topic_ids;
    if (comm::RetCode::RET_OK !=
        (ret = config::GlobalConfig::GetThreadInstance()->GetAllTopicID(topic_ids))) {
        QLErr("GetAllTopicID ret %d", as_integer(ret));
        return ret;
    }

    for (auto &&topic_id : topic_ids) {

        QLVerb("check topic_id %d", topic_id);

        shared_ptr<const config::StoreConfig> store_config;
        if (comm::RetCode::RET_OK !=
            (ret = config::GlobalConfig::GetThreadInstance()->
             GetStoreConfig(topic_id, store_config))) {
            QLErr("GetStoreConfig ret %d", as_integer(ret));
            continue;
        }

        int store_id;
        if (comm::RetCode::RET_OK ==
            (ret = store_config->GetStoreIDByAddr(impl_->addr, store_id))) {
            QLInfo("found toipc_id %d addr (%s:%d)",
                   topic_id, impl_->addr.ip().c_str(), impl_->addr.port());
            impl_->topic_id = topic_id;
            return comm::RetCode::RET_OK;
        }
    }

    return comm::RetCode::RET_ERR_RANGE_ADDR;
}

comm::RetCode Store::Init() {
    comm::RetCode ret;

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

    if (!comm::utils::AccessDir(impl_->opt.data_dir_path)) {
        QLErr("path %s not exist", impl_->opt.data_dir_path.c_str());
        return comm::RetCode::RET_DIR_NOT_EXIST;
    }

    impl_->addr.set_ip(impl_->opt.ip);
    impl_->addr.set_port(impl_->opt.port);
    impl_->addr.set_paxos_port(impl_->opt.paxos_port);

    if (comm::RetCode::RET_OK != (ret = InitTopicID())) {
        QLErr("InitTopicID ret %d topic %s", as_integer(ret), impl_->opt.topic.c_str());
        return ret;
    }

    comm::StoreBP::GetThreadInstance()->OnInit(impl_->topic_id);

    QLVerb("ip %s port %d paxos_port %d", impl_->opt.ip.c_str(),
           impl_->opt.port, impl_->opt.paxos_port);

    impl_->basemgr = unique_ptr<BaseMgr>(new BaseMgr(this));
    if (comm::RetCode::RET_OK != (ret = impl_->basemgr->Init())) {
        comm::StoreBP::GetThreadInstance()->OnInitErrBaseMgr(impl_->topic_id);
        QLErr("basemgr Init ret %d", as_integer(ret));
        return ret;
    }

    impl_->sync = unique_ptr<SyncCtrl>(new SyncCtrl(this));
    if (comm::RetCode::RET_OK != (ret = impl_->sync->Init())) {
        comm::StoreBP::GetThreadInstance()->OnInitErrSyncCtrl(impl_->topic_id);
        QLErr("sync Init ret %d", as_integer(ret));
        return ret;
    }

    impl_->cpmgr = unique_ptr<CheckPointStatMgr>(new CheckPointStatMgr(this));
    if (comm::RetCode::RET_OK != (ret = impl_->cpmgr->Init())) {
        comm::StoreBP::GetThreadInstance()->OnInitErrCPMgr(impl_->topic_id);
        QLErr("cpmgr Init ret %d", as_integer(ret));
        return ret;
    }

    impl_->keep_sync_thread = unique_ptr<KeepSyncThread>(new KeepSyncThread(this));
    impl_->keep_master_thread = unique_ptr<KeepMasterThread>(new KeepMasterThread(this));

    if (comm::RetCode::RET_OK != (ret = PaxosInit())) {
        comm::StoreBP::GetThreadInstance()->OnInitErrPaxos(impl_->topic_id);
        QLErr("PaxosInit ret %d", as_integer(ret));
        return ret;
    }

    impl_->keep_sync_thread->Run();
    impl_->keep_master_thread->Run();

    comm::StoreBP::GetThreadInstance()->OnInitSucc(impl_->topic_id);

    return comm::RetCode::RET_OK;
}

int Store::GetTopicID() {
    return impl_->topic_id;
}

const comm::proto::Addr &Store::GetAddr() {
    return impl_->addr;
}

comm::RetCode Store::PaxosInit() {
    comm::RetCode ret;

    phxpaxos::Options opts;

    // 1. set members

    auto topic_id(impl_->topic_id);

    shared_ptr<const config::TopicConfig> topic_config;
    if (comm::RetCode::RET_OK !=
        (ret = config::GlobalConfig::GetThreadInstance()->
         GetTopicConfigByTopicID(topic_id, topic_config))) {
        QLErr("GetTopicConfig ret %d topic_id %d", as_integer(ret), topic_id);
        return ret;
    }

    shared_ptr<const config::StoreConfig> store_config;
    if (comm::RetCode::RET_OK !=
        (ret = config::GlobalConfig::GetThreadInstance()->
         GetStoreConfig(topic_id, store_config))) {
        QLErr("GetStoreConfig ret %d topic_id %d", as_integer(ret), topic_id);
        return ret;
    }

    QLVerb("addr(%s:%d:%d)", impl_->addr.ip().c_str(),
           impl_->addr.port(), impl_->addr.paxos_port());

    shared_ptr<const config::proto::Store> store;
    if (comm::RetCode::RET_OK != (ret = store_config->GetStoreByAddr(impl_->addr, store))) {
        QLErr("GetStoreByAddr ret %d", as_integer(ret));
        return ret;
    }

    for (size_t i{0}; i < store->addrs_size(); ++i) {
        auto &&addr(store->addrs(i));
        opts.vecNodeInfoList.emplace_back(addr.ip(), addr.paxos_port());
    }


    // 2. set nr_group
    opts.iGroupCount = impl_->opt.ngroup;
    for (int i{0}; i < opts.iGroupCount; ++i) {
        phxpaxos::GroupSMInfo sminfo;
        sminfo.iGroupIdx = i;
        unique_ptr<StoreSM> sm(new StoreSM(this));
        sminfo.vecSMList.push_back(sm.get());
        sminfo.bIsUseMaster = true;
        opts.vecGroupSMInfoList.push_back(sminfo);

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

    opts.bUseCheckpointReplayer = false;
    opts.iSyncInterval = topic_config->GetProto().topic().store_paxos_fsync_interval();
    opts.bUseBatchPropose = true;
    opts.iIOThreadCount = impl_->opt.npaxos_iothread;


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

    return comm::RetCode::RET_OK;
}

Node *Store::GetNode() {
    return impl_->node.get();
}

BaseMgr *Store::GetBaseMgr() {
    return impl_->basemgr.get();
}

SyncCtrl *Store::GetSyncCtrl() {
    return impl_->sync.get();
}

CheckPointStatMgr *Store::GetCheckPointStatMgr() {
    return impl_->cpmgr.get();
}

comm::RetCode Store::CheckRequestComm(const int store_id, const int queue_id) {
    auto topic_id(impl_->topic_id);

    comm::RetCode ret;

    shared_ptr<const config::StoreConfig> store_config;
    if (comm::RetCode::RET_OK !=
        (ret = config::GlobalConfig::GetThreadInstance()->
         GetStoreConfig(topic_id, store_config))) {
        QLErr("GetStoreConfig ret %d topic_id %d", as_integer(ret), topic_id);
        return ret;
    }

    shared_ptr<const config::proto::Store> store;
    if (comm::RetCode::RET_OK != (ret = store_config->GetStoreByAddr(impl_->addr, store))) {
        QLErr("GetStoreByAddr ret %d", as_integer(ret));
        return ret;
    }

    // check store_id
    if (store->store_id() != store_id) {
        QLErr("store_id unmatch. store_id %d req_store_id %d", store->store_id(), store_id);
        return comm::RetCode::RET_ERR_RANGE_STORE;
    }

    shared_ptr<const config::TopicConfig> topic_config;
    if (comm::RetCode::RET_OK !=
        (ret = config::GlobalConfig::GetThreadInstance()->
         GetTopicConfigByTopicID(topic_id, topic_config))) {
        QLErr("GetTopicConfigByTopicID ret %d", as_integer(ret));
        return ret;
    }

    // check queue_id
    if (!topic_config->IsValidQueue(queue_id)) {
        QLErr("IsValidQueue fail. queue_id %d", queue_id);
        return comm::RetCode::RET_ERR_RANGE_PUB;
    }

    return comm::RetCode::RET_OK;

}


comm::RetCode Store::CheckAddRequest(const comm::proto::AddRequest &req) {

    auto topic_id(impl_->topic_id);

    comm::RetCode ret;

    if (comm::RetCode::RET_OK != (ret = CheckRequestComm(req.store_id(), req.queue_id()))) {
        QLErr("CheckRequestComm ret %d", as_integer(ret));
        return ret;
    }


    shared_ptr<const config::TopicConfig> topic_config;
    if (comm::RetCode::RET_OK !=
        (ret = config::GlobalConfig::GetThreadInstance()->
         GetTopicConfigByTopicID(topic_id, topic_config))) {
        QLErr("GetTopicConfigByTopicID ret %d", as_integer(ret));
        return ret;
    }

    size_t byte_size{0};
    for (int i{0}; i < req.items_size(); ++i) {
        byte_size += req.items(i).ByteSize();
    }

    if (byte_size > topic_config->GetProto().topic().items_byte_size_limit()) {
        QLErr("size too large. byte_size %d >= size_limit %d",
              req.ByteSize(), topic_config->GetProto().topic().items_byte_size_limit());
        return comm::RetCode::RET_ERR_SIZE_TOO_LARGE;
    }

    return comm::RetCode::RET_OK;
}

comm::RetCode Store::CheckGetRequest(const comm::proto::GetRequest &req) {

    auto topic_id(impl_->topic_id);

    comm::RetCode ret;

    if (comm::RetCode::RET_OK != (ret = CheckRequestComm(req.store_id(), req.queue_id()))) {
        QLErr("CheckRequestComm ret %d", as_integer(ret));
        return ret;
    }

    shared_ptr<const config::TopicConfig> topic_config;
    if (comm::RetCode::RET_OK !=
        (ret = config::GlobalConfig::GetThreadInstance()->
         GetTopicConfigByTopicID(topic_id, topic_config))) {
        QLErr("GetTopicConfigByTopicID ret %d", as_integer(ret));
        return ret;
    }

    if (topic_config->QueueShouldSkip(req.queue_id(), req.consumer_group_id())) {
        return comm::RetCode::RET_ERR_RANGE;
    }

    return comm::RetCode::RET_OK;
}


comm::RetCode Store::CheckMaster(const int paxos_group_id, comm::proto::Addr &redirect_addr) {
    QLVerb("CheckMaster");

    comm::RetCode ret;

    if (impl_->node->IsIMMaster(paxos_group_id))
        return comm::RetCode::RET_OK;

    auto &&node_info = impl_->node->GetMaster(paxos_group_id);
    if (node_info.GetIP().empty() || 0 == node_info.GetPort() || "0.0.0.0" == node_info.GetIP())
        return comm::RetCode::RET_ERR_NO_MASTER;

    auto &&topic_id(impl_->topic_id);

    shared_ptr<const config::StoreConfig> store_config;
    if (comm::RetCode::RET_OK !=
        (ret = config::GlobalConfig::GetThreadInstance()->
         GetStoreConfig(topic_id, store_config))) {
        QLErr("GetStoreConfigByPubID ret %d topic_id %d", as_integer(ret), topic_id);
        return ret;
    }

    shared_ptr<const config::proto::Store> store;
    if (comm::RetCode::RET_OK != (ret = store_config->GetStoreByAddr(impl_->addr, store))) {
        QLErr("GetStoreByAddr ret %d", as_integer(ret));
        return ret;
    }

    QLVerb("find master");
    for (size_t i{0}; i < store->addrs_size(); ++i) {
        auto &&tmp_addr = store->addrs(i);
        if (tmp_addr.ip() == node_info.GetIP() &&
            tmp_addr.paxos_port() == node_info.GetPort()) {
            redirect_addr.CopyFrom(tmp_addr);
            QLVerb("master found. redirect_addr(%s:%d)",
                   redirect_addr.ip().c_str(), redirect_addr.port());

            return comm::RetCode::RET_ERR_NOT_MASTER;
        }
    }
    return comm::RetCode::RET_ERR_RANGE_ADDR;
}


comm::RetCode Store::Add(const comm::proto::AddRequest &req, comm::proto::AddResponse &resp) {
    QLVerb("Add");

    comm::RetCode ret;

    comm::StoreBP::GetThreadInstance()->OnAdd(req);

    if (comm::RetCode::RET_OK != (ret = CheckAddRequest(req))) {
        comm::StoreBP::GetThreadInstance()->OnAddRequestInvalid(req);
        QLErr("CheckAddRequest ret %d", as_integer(ret));
        return ret;
    }

    if (SkipAdd(req)) {
        return comm::RetCode::RET_OK;
    }

    int paxos_group_id = req.queue_id() % impl_->opt.ngroup;
    if (comm::RetCode::RET_OK !=
        (ret = CheckMaster(paxos_group_id, *resp.mutable_redirect_addr()))) {
        comm::StoreBP::GetThreadInstance()->OnAddCheckMasterUnpass(req);
        if (as_integer(ret) < 0) {
            QLErr("CheckMaster ret %d paxos_group_id %d", as_integer(ret), paxos_group_id);
        }
        return ret;
    }
    comm::StoreBP::GetThreadInstance()->OnAddCheckMasterPass(req);

    ret = PaxosAdd(req, resp);
    if (comm::RetCode::RET_OK != ret) {
        QLErr("PaxosAdd ret %d", as_integer(ret));
    }

    QLVerb("Add succ. req: topic_id %d store_id %d queue_id %d "
           "items.size() %d. resp: cursor_id %" PRIu64,
           req.topic_id(), req.store_id(), req.queue_id(), req.items_size(),
           resp.cursor_id());
    for (int i{0}; i < req.items_size(); i++) {
        auto &&item = req.items(i);
        QLVerb("item: topic_id %d uin %" PRIu64, item.meta().topic_id(), item.meta().uin());
    }

    return ret;
}

comm::RetCode Store::PaxosAdd(const comm::proto::AddRequest &req, comm::proto::AddResponse &resp) {
    comm::StoreBP::GetThreadInstance()->OnPaxosAdd(req);

    int paxos_group_id{req.queue_id() % impl_->opt.ngroup};

    proto::StorePaxosArgs args;
    args.set_timestamp(time(nullptr));
    args.mutable_add_req()->CopyFrom(req);

    string buf;
    args.SerializeToString(&buf);

    StoreContext sc;
    sc.result = comm::RetCode::RET_OK;

    comm::StoreBP::GetThreadInstance()->OnBatchPropose(req);

    uint64_t instance_id;
    uint32_t batch_idx;
    phxpaxos::SMCtx ctx(StoreSM::ID, &sc);

    uint64_t t1{comm::utils::Time::GetSteadyClockMS()};
    int paxos_ret{impl_->node->BatchPropose(paxos_group_id, buf, instance_id, batch_idx, &ctx)};
    uint64_t t2{comm::utils::Time::GetSteadyClockMS()};
    uint64_t used_time_ms{t2 - t1};

    if (phxpaxos::PaxosTryCommitRet_OK != paxos_ret) {
        comm::StoreBP::GetThreadInstance()->OnBatchProposeErr(req, used_time_ms);
        QLErr("BatchPropose paxos_ret %d paxos_group_id %d buf_length %zu",
              paxos_ret, paxos_group_id, buf.length());
        switch (paxos_ret) {
        case phxpaxos::PaxosTryCommitRet_Timeout:
            comm::StoreBP::GetThreadInstance()->OnBatchProposeErrTimeout(req);
            return comm::RetCode::RET_ERR_PROPOSE_TIMEOUT;
        case phxpaxos::PaxosTryCommitRet_TooManyThreadWaiting_Reject:
            comm::StoreBP::GetThreadInstance()->OnBatchProposeErrTooManyThreadWaitingReject(req);
            return comm::RetCode::RET_ERR_PROPOSE_FAST_REJECT;
        case phxpaxos::PaxosTryCommitRet_Value_Size_TooLarge:
            comm::StoreBP::GetThreadInstance()->OnBatchProposeErrValueSizeTooLarge(req);
            return comm::RetCode::RET_ERR_SIZE_TOO_LARGE;
        default:
            comm::StoreBP::GetThreadInstance()->OnBatchProposeErrOther(req);
            return comm::RetCode::RET_ERR_PROPOSE;
        };
    }
    comm::StoreBP::GetThreadInstance()->
            OnBatchProposeSucc(req, instance_id, batch_idx, used_time_ms);


    resp.set_cursor_id(instance_id);

    return sc.result;
}

comm::RetCode Store::Get(const comm::proto::GetRequest &req, comm::proto::GetResponse &resp) {
    QLVerb("Get");

    comm::StoreBP::GetThreadInstance()->OnGet(req);

    comm::RetCode ret;

    if (comm::RetCode::RET_OK != (ret = CheckGetRequest(req))) {
        comm::StoreBP::GetThreadInstance()->OnGetRequestInvalid(req);
        QLErr("CheckGetRequest ret %d", as_integer(ret));
        return ret;
    }

    int paxos_group_id{req.queue_id() % impl_->opt.ngroup};
    if (comm::RetCode::RET_OK !=
        (ret = CheckMaster(paxos_group_id, *resp.mutable_redirect_addr()))) {
        comm::StoreBP::GetThreadInstance()->OnGetCheckMasterUnpass(req);
        if (as_integer(ret) < 0) {
            QLErr("CheckMaster ret %d paxos_group_id %d", as_integer(ret), paxos_group_id);
        }
        return ret;
    }
    comm::StoreBP::GetThreadInstance()->OnGetCheckMasterPass(req);

    comm::StoreBP::GetThreadInstance()->OnBaseMgrGet(req);
    if (comm::RetCode::RET_OK != (ret = impl_->basemgr->Get(req, resp))) {
        comm::StoreBP::GetThreadInstance()->OnBaseMgrGetFail(req);
        QLErr("basemgr Get ret %d", as_integer(ret));
        return ret;
    }
    comm::StoreBP::GetThreadInstance()->OnBaseMgrGetSucc(req, resp);

    QLVerb("Get Succ. "
           "req: topic_id %d store_id %d queue_id %d limit %d atime %" PRIu64
           " consumer_group_id %d prev_cursor_id %" PRIu64 " next_cursor_id %" PRIu64 ". "
           "resp: items.size() %d prev_cursor_id %" PRIu64 " next_cursor_id %" PRIu64,
           req.topic_id(), req.store_id(), req.queue_id(), req.limit(),
           req.atime(), req.consumer_group_id(), req.prev_cursor_id(), req.next_cursor_id(),
           resp.items_size(), resp.prev_cursor_id(), resp.next_cursor_id());

    return comm::RetCode::RET_OK;
}

bool Store::SkipAdd(const comm::proto::AddRequest &req) {
    if (0 == req.items_size()) return true;

    comm::RetCode ret;

    shared_ptr<const config::TopicConfig> topic_config;
    if (comm::RetCode::RET_OK != (ret = config::GlobalConfig::GetThreadInstance()->GetTopicConfigByTopicID(req.topic_id(), topic_config))) {
        QLErr("GetTopicConfig ret %d", as_integer(ret));
        return true;
    }

    if (!topic_config->IsValidQueue(req.queue_id())) {
        QLErr("IsValidQueue fail. queue_id %d", req.queue_id());
        return true;
    }

    if (topic_config->QueueShouldSkip(req.queue_id())) {
        QLInfo("QueueShouldSkip. queue_id %d", req.queue_id());
        return true;
    }

    return false;
}

bool Store::SkipGet(const comm::proto::QItem &item, const comm::proto::GetRequest &req) {
    comm::RetCode ret;

    if (item.meta().topic_id() != req.topic_id()) {
        QLErr("get item skip. item.topic %d req.topic %d",
              item.meta().topic_id(), req.topic_id());
        return true;
    }

    if (0 == ((1uLL << (req.consumer_group_id() - 1)) & item.consumer_group_ids())) {
        return true;
    }

    return false;
}

bool Store::NeedDropMaster(const int paxos_group_id) {
    return false;
}


}  // namespace  store

}  // namespace phxqueue


//gzrd_Lib_CPP_Version_ID--start
#ifndef GZRD_SVN_ATTR
#define GZRD_SVN_ATTR "0"
#endif
static char gzrd_Lib_CPP_Version_ID[] __attribute__((used))="$HeadURL$ $Id$ " GZRD_SVN_ATTR "__file__";
// gzrd_Lib_CPP_Version_ID--end

