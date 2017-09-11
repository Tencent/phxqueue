/*
Tencent is pleased to support the open source community by making PhxQueue available.
Copyright (C) 2017 THL A29 Limited, a Tencent company. All rights reserved.
Licensed under the BSD 3-Clause License (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at

<https://opensource.org/licenses/BSD-3-Clause>

Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.
*/



#pragma once

#include "phxqueue/comm.h"

#include "phxqueue/scheduler/scheduleroption.h"


namespace phxqueue {

namespace scheduler {


class SchedulerMgr;


class Scheduler {
  public:
    Scheduler(const SchedulerOption &opt);
    virtual ~Scheduler();

    // ------------------------ Interfaces generally used ------------------------

    // Init.
    // Usage please refer to phxqueue_phxrpc/app/scheduler_main.cpp
    comm::RetCode Init();

    // Collect consumers' cpu load, and return dynamic weight of each consumer.
    // Usage please refer to phxqueue_phxrpc/app/scheduler_service_impl.cpp
    comm::RetCode GetAddrScale(const comm::proto::GetAddrScaleRequest &req,
                               comm::proto::GetAddrScaleResponse &resp);

    // ------------------------ Interfaces MUST be overrided ------------------------
    // Implement please refer to phxqueue_phxrpc/scheduler/scheduler.cpp

  protected:

    // According to specific lock key, get lock infomation from Lock.
    // Such as version, lease time, who is holding the lock. Refer to LockInfo in phxqueue/comm/proto/comm.proto
    // Need to implement an RPC that corresponds to Lock::GetLockInfo().
    virtual comm::RetCode GetLockInfo(const comm::proto::GetLockInfoRequest &req,
                                      comm::proto::GetLockInfoResponse &resp) = 0;

    // According to the lock information returned by GetLockInfo, after a series of distributed lock logic, decide whether to lease or renew this lock by AcquireLock.
    // Need to implement an RPC that corresponds to Lock::AcquireLock().
    virtual comm::RetCode AcquireLock(const comm::proto::AcquireLockRequest &req,
                                      comm::proto::AcquireLockResponse &resp) = 0;

    // ------------------------ Interfaces CAN be overrided ------------------------

    // If returned True, store will abandon leasing master of the group specified by pxaos_group_id.
    // You can implement your own HA strategy here.
    virtual bool NeedDropMaster();

    // If returned True, scheduler will skip updating consumer's load, just return dynamic weight.
    virtual bool NeedSkipUpdateLoad(const comm::proto::GetAddrScaleRequest &req);

    // Callback on runing the thread of keeping master.
    virtual void OnKeepMasterThreadRun();

    // ------------------------ Interfaces used internally ------------------------

    // Return SchedulerOption.
    const SchedulerOption *GetSchedulerOption() const;

    // Dispost.
    comm::RetCode Dispose();

    // Return addr of this lock instance.
    const comm::proto::Addr &GetAddr();

    // Return SchedulerMgr instance. 
    SchedulerMgr *GetSchedulerMgr();

    // Determine topic id.
    comm::RetCode InitTopicID();

    // Return topic id.
    int GetTopicID();


    // Wrapper of GetLockInfo.
    comm::RetCode RawGetLockInfo(comm::proto::GetLockInfoRequest &req,
                                 comm::proto::GetLockInfoResponse &resp);

    // Wrapper of RawAcquireLock.
    comm::RetCode RawAcquireLock(comm::proto::AcquireLockRequest &req,
                                 comm::proto::AcquireLockResponse &resp);

  private:
    class SchedulerImpl;
    std::unique_ptr<SchedulerImpl> impl_;

    friend class KeepMasterThread;
    friend class LoadBalanceThread;
    friend class SchedulerMgr;

};


}  // namespace scheduler

}  // namespace phxqueue

