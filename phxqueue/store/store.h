/*
Tencent is pleased to support the open source community by making PhxQueue available.
Copyright (C) 2017 THL A29 Limited, a Tencent company. All rights reserved.
Licensed under the BSD 3-Clause License (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at

<https://opensource.org/licenses/BSD-3-Clause>

Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.
*/



#pragma once

#include <memory>

#include "phxqueue/comm.h"

#include "phxqueue/store/storeoption.h"


namespace phxpaxos {


class Node;
class Options;


}


namespace phxqueue {

namespace store {


class BaseMgr;
class SyncCtrl;
class CheckPointStatMgr;


class StoreContext final {
  public:
    StoreContext() {
        result = comm::RetCode::RET_OK;
    }
    ~StoreContext() {}

    comm::RetCode result;
};


class Store {
  public:
    Store(const StoreOption &opt);
    virtual ~Store();

    // ------------------------ Interfaces generally used ------------------------

    // Init.
    // Usage please refer to phxqueue_phxrpc/test/store_main.cpp
    comm::RetCode Init();

    // Add items into queue.
    // Usage please refer to phxqueue_phxrpc/app/store/store_service_impl.cpp
    comm::RetCode Add(const comm::proto::AddRequest &req, comm::proto::AddResponse &resp);

    // Get items from queue.
    // Usage please refer to phxqueue_phxrpc/app/store/store_service_impl.cpp
    comm::RetCode Get(const comm::proto::GetRequest &req, comm::proto::GetResponse &resp);


  protected:

    // ------------------------ Interfaces CAN be overrided ------------------------

    // Implement to skip Add.
    virtual bool SkipAdd(const comm::proto::AddRequest &req);

    // Implement to skip some items on Get.
    virtual bool SkipGet(const comm::proto::QItem &item, const comm::proto::GetRequest &req);

    // If returned True, store will abandon leasing master of the group specified by pxaos_group_id.
    // You can implement your own HA strategy here.
    virtual bool NeedDropMaster(const int paxos_group_id);

    // Callback before the paxos processing starts, to modify phxpaxos::Options.
    virtual void BeforeRunNode(phxpaxos::Options &opts) {}

    // ------------------------ Interfaces used internally ------------------------

    // Return StoreOption.
    const StoreOption *GetStoreOption() const;

    // Determine topic id.
    comm::RetCode InitTopicID();

    // Return topic id.
    int GetTopicID();

    // Return addr of this store instance.
    const comm::proto::Addr &GetAddr();

    // Return phxpaxos::Node instance, to do propose.
    phxpaxos::Node *GetNode();

    // Return BaseMgr instance, to maintain the stat of queues on memory. 
    BaseMgr *GetBaseMgr();

    // Return SyncCtrl, to maintain the read offset in synchronization between the main standby machine.
    SyncCtrl *GetSyncCtrl();

    // Return CheckPointStatMgr, to maintain checkpoint of paxos.
    CheckPointStatMgr *GetCheckPointStatMgr();

    // Init phxpaxos::Node.
    comm::RetCode PaxosInit();

    // Common check logic of request in Add/Get.
    comm::RetCode CheckRequestComm(const int store_id, const int queue_id);

    // Check logic of request in Add.
    comm::RetCode CheckAddRequest(const comm::proto::AddRequest &req);

    // Check logic of request in Get.
    comm::RetCode CheckGetRequest(const comm::proto::GetRequest &req);

    // The first thing to do when a request arrived is to check if the current store instance is master, and if not, redirect to the master.
    comm::RetCode CheckMaster(const int paxos_group_id, comm::proto::Addr &redirect_addr);

    // Process propose of paxos.
    comm::RetCode PaxosAdd(const comm::proto::AddRequest &req, comm::proto::AddResponse &resp);

  private:
    class StoreImpl;
    std::unique_ptr<StoreImpl> impl_;

    friend class BaseMgr;
    friend class SyncCtrl;
    friend class CheckPointStat;
    friend class CheckPointStatMgr;
    friend class StoreSM;
    friend class KeepMasterThread;
    friend class KeepSyncThread;
};



}  // namespace store

}  // namespace phxqueue

