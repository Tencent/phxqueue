/*
Tencent is pleased to support the open source community by making PhxQueue available.
Copyright (C) 2017 THL A29 Limited, a Tencent company. All rights reserved.
Licensed under the BSD 3-Clause License (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at

<https://opensource.org/licenses/BSD-3-Clause>

Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.
*/



#pragma once

#include "phxqueue/comm.h"
#include "phxqueue/lock/lockoption.h"


namespace phxpaxos {


class Node;
class Options;


}


namespace phxqueue {

namespace lock {


class LockMgr;


class LockContext {
  public:
    LockContext() {
        result = comm::RetCode::RET_OK;
    }
    virtual ~LockContext() {}

    comm::RetCode result;
};


class Lock {
  public:
    Lock(const LockOption &opt);
    virtual ~Lock();

    // ------------------------ Interfaces generally used ------------------------

    // Init.
    // Usage please refer to phxqueue_phxrpc/app/lock/lock_main.cpp
    comm::RetCode Init();


    // Distributed lock pseudocode:

    // server:
    // rpc GetLockInfo(a_LockID) return (Version, ClientID, LiveTime)
    //     return Version[a_LockID], ClientID[a_LockID], LiveTime[a_LockID]
    // rpc AcquireLock(a_ClientID, a_LockID, a_Version, a_LiveTime) return (Succ)
    //     if a_Version != Version[a_LockID]:
    //         retrurn False
    //     Update(CurTime)
    //     Version[a_LockID] := a_Version + 1
    //     ClientID[a_LockID] := a_ClientID
    //     LiveTime[a_LockID] := a_LiveTime
    //     OverdueTime[a_LockID] := CurTime + a_LiveTime
    //     return True

    // client:
    // function TryLock() return (Succ)
    //     Update(CurTime)
    //     PeerVersion, PeerClientID, PeerLiveTime := GetLockInfo(LockID)
    //     if Version != PeerVersion:
    //         Version := PeerVersion
    //         OverdueTime := CurTime + PeerLiveTime
    //     Update(LiveTime)
    //     if ClientID = PeerClientID || CurTime > OverdueTime:
    //         Succ := TryLock(ClientID, LockID, Version, LiveTime)
    //         if Succ:
    //             return True
    //     return False

    comm::RetCode GetString(const comm::proto::GetStringRequest &req,
                            comm::proto::GetStringResponse &resp);
    comm::RetCode SetString(const comm::proto::SetStringRequest &req,
                            comm::proto::SetStringResponse &resp);
    comm::RetCode DeleteString(const comm::proto::DeleteStringRequest &req,
                               comm::proto::DeleteStringResponse &resp);

    // Usage please refer to phxqueue_phxrpc/app/lock/lock_service_impl.cpp
    comm::RetCode GetLockInfo(const comm::proto::GetLockInfoRequest &req,
                              comm::proto::GetLockInfoResponse &resp);

    // Usage please refer to phxqueue_phxrpc/app/lock/lock_service_impl.cpp
    comm::RetCode AcquireLock(const comm::proto::AcquireLockRequest &req,
                              comm::proto::AcquireLockResponse &resp);

    // ------------------------ Interfaces CAN be overrided ------------------------

  protected:

    // Callback before the paxos processing starts, to modify phxpaxos::Options.
    virtual void BeforeRunNode(phxpaxos::Options &opts) {}

    // ------------------------ Interfaces used internally ------------------------

  public:

    // If returned True, store will abandon leasing master of the group specified by pxaos_group_id.
    // You can implement your own HA strategy here.
    virtual bool NeedDropMaster(const int paxos_group_id);

    // Return phxpaxos::Node instance, to do propose.
    phxpaxos::Node *GetNode();

    // Return LockMgr instance, to process the pseudocode mentioned above. 
    LockMgr *GetLockMgr();

    // Return LockOption.
    const LockOption *GetLockOption() const;

    // Return topic id.
    int GetTopicID();

    // Return addr of this lock instance.
    const comm::proto::Addr &GetAddr();

  protected:

    // Dispose.
    comm::RetCode Dispose();

    // Init phxpaxos::Node.
    comm::RetCode PaxosInit(const std::string &mirror_dir_path);

    // The first thing to do when a request arrived is to check if the current store instance is master, and if not, redirect to the master.
    comm::RetCode CheckMaster(const int paxos_group_id, comm::proto::Addr &redirect_addr);

    comm::RetCode PaxosSetString(const comm::proto::SetStringRequest &req,
                                 comm::proto::SetStringResponse &resp);

    comm::RetCode PaxosDeleteString(const comm::proto::DeleteStringRequest &req,
                                    comm::proto::DeleteStringResponse &resp);

    // Process the pseudocode of AcquireLock mentioned above.
    comm::RetCode PaxosAcquireLock(const comm::proto::AcquireLockRequest &req,
                                   comm::proto::AcquireLockResponse &resp);

    // Determine topic id.
    comm::RetCode InitTopicID();

    class LockImpl;
    std::unique_ptr<LockImpl> impl_;
};




}  // namespace lock

}  // namespace phxqueue

