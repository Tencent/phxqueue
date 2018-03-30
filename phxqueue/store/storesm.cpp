/*
Tencent is pleased to support the open source community by making PhxQueue available.
Copyright (C) 2017 THL A29 Limited, a Tencent company. All rights reserved.
Licensed under the BSD 3-Clause License (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at

<https://opensource.org/licenses/BSD-3-Clause>

Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.
*/



#include "phxqueue/store/storesm.h"

#include <cinttypes>
#include <unistd.h>

#include "phxqueue/comm.h"
#include "phxpaxos/node.h"

#include "phxqueue/store/store.h"
#include "phxqueue/store/basemgr.h"
#include "phxqueue/store/storemeta.h"
#include "phxqueue/store/syncctrl.h"


namespace phxqueue {

namespace store {


using namespace std;


class StoreSM::StoreSMImpl {
  public:
    StoreSMImpl() {}
    virtual ~StoreSMImpl() {}

    Store *store{nullptr};
};

StoreSM::StoreSM(Store *const store) : impl_(new StoreSMImpl()) {
    impl_->store = store;
}

StoreSM::~StoreSM() {}

bool StoreSM::Execute(const int paxos_group_id, const uint64_t instance_id,
                      const string &paxos_value, phxpaxos::SMCtx *ctx) {
    QLVerb("StoreSM::Execute begin");
    QLVerb("paxos_group_id %d instance_id %" PRIu64 " ctx %p paxos_value.length %zu",
           paxos_group_id, instance_id, ctx, paxos_value.length());

    comm::RetCode ret;

    StoreContext *sc{nullptr};
    if (ctx && ctx->m_pCtx) {
        sc = static_cast<StoreContext *>(ctx->m_pCtx);
    }

    proto::StorePaxosArgs args;
    if (!args.ParseFromString(paxos_value)) {
        QLErr("StorePaxosArgs ParseFromString err. pv.length %zu", paxos_value.length());
        if (sc) sc->result = comm::RetCode::RET_ERR_ARG;
        return false;
    }

    const uint64_t cursor_id{instance_id};
    if (args.has_add_req()) {
        QLVerb("value.length() %zu", paxos_value.length());
        if (0 > as_integer(ret = impl_->store->GetBaseMgr()->Add(cursor_id, args.add_req()))) {
            QLErr("BaseMgr::Add err %d", as_integer(ret));
            if (sc) sc->result = ret;
            return false;
        }
    }

    if (args.has_sync_ctrl_info()) {
        if (0 > as_integer(ret = impl_->store->GetSyncCtrl()->SyncCursorID(args.sync_ctrl_info()))) {
            QLErr("SyncCtrl::SyncCursorID ret %d", as_integer(ret));
            if (sc) sc->result = ret;
            return false;
        }
    }

    if (!args.master_addr().ip().empty()) {
        auto opt(impl_->store->GetStoreOption());

        comm::proto::Addr addr;
        addr.set_ip(opt->ip);
        addr.set_port(opt->port);
        addr.set_paxos_port(opt->paxos_port);

        if (!(addr.ip() == args.master_addr().ip() &&
              addr.port() == args.master_addr().port() &&
              addr.paxos_port() == args.master_addr().paxos_port())) {

            // 1. drop master
            QLInfo("drop master. addr.ip %s master_addr.ip %s", addr.ip().c_str(), args.master_addr().ip().c_str());
            if (nullptr != impl_->store->GetNode()) { // GetNode() still return nullptr while calling RunNode() in Store::PaxosInit()
                impl_->store->GetNode()->DropMaster(paxos_group_id);
            }
        }
    }

    if (sc) sc->result = comm::RetCode::RET_OK;
    QLVerb("StoreSM::Execute end");

    return true;
}

const uint64_t StoreSM::GetCheckpointInstanceID(const int paxos_group_id) const {
    QLVerb("StoreSM::GetCheckpointInstanceID begin");

    uint64_t cp(-1);

    comm::RetCode ret;

    auto stat(impl_->store->GetCheckPointStatMgr()->GetCheckPointStat(paxos_group_id));
    if (!stat) {
        QLErr("CheckPointStatMgr::GetCheckPointStat fail paxos_group_id %d", paxos_group_id);
        return -1;
    }
    if (comm::RetCode::RET_OK != (ret = stat->GetCheckPoint(cp))) {
        QLErr("GetCheckPoint fail ret %d", as_integer(ret));
        return -1;
    }
    QLVerb("paxos_group_id %d cp %" PRIu64, paxos_group_id, cp);


    comm::StoreSMBP::GetThreadInstance()->OnGetCheckpointInstanceID(paxos_group_id, cp);

    return cp;
}


int StoreSM::GetCheckpointState(const int paxos_group_id, string &dir_path,
                                vector<string> &file_list) {
    QLVerb("StoreSM::GetCheckpointState begin");

    auto stat(impl_->store->GetCheckPointStatMgr()->GetCheckPointStat(paxos_group_id));
    if (!stat) {
        QLErr("GetCheckPointStat fail paxos_group_id %d", paxos_group_id);
        return -1;
    }

    dir_path = stat->GetDir();
    file_list.clear();
    file_list.push_back(stat->GetFile());

    return 0;
}

int StoreSM::LoadCheckpointState(const int paxos_group_id, const string &tmp_dir_path,
                                 const vector<string> &vecFileList, const uint64_t cp) {
    QLVerb("StoreSM::LoadCheckpointState begin");

    comm::RetCode ret;
    QLErr("paxos_group_id %d cp %" PRIu64, paxos_group_id, cp);

    if (cp == -1) return 0;

    auto stat = impl_->store->GetCheckPointStatMgr()->GetCheckPointStat(paxos_group_id);
    if (!stat) {
        QLErr("GetCheckPointStat fail paxos_group_id %d", paxos_group_id);
        return -1;
    }

    if (comm::RetCode::RET_OK != (ret = stat->UpdateCheckPointAndFlush(cp))) {
        QLErr("UpdateCheckPointAndFlush ret %d paxos_group_id %d cp %" PRIu64,
              ret, paxos_group_id, cp);
        return -2;
    }

    sleep(10);  // wait for other groups doing LoadCheckpointState

    return 0;
}


}  // namespace store

}  // namespace phxqueue


//gzrd_Lib_CPP_Version_ID--start
#ifndef GZRD_SVN_ATTR
#define GZRD_SVN_ATTR "0"
#endif
static char gzrd_Lib_CPP_Version_ID[] __attribute__((used))="$HeadURL$ $Id$ " GZRD_SVN_ATTR "__file__";
// gzrd_Lib_CPP_Version_ID--end

