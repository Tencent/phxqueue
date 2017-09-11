/*
Tencent is pleased to support the open source community by making PhxQueue available.
Copyright (C) 2017 THL A29 Limited, a Tencent company. All rights reserved.
Licensed under the BSD 3-Clause License (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at

<https://opensource.org/licenses/BSD-3-Clause>

Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.
*/



#include "phxqueue_phxrpc/scheduler/scheduler.h"

#include <cinttypes>

#include "phxqueue/comm.h"

#include "phxqueue_phxrpc/app/lock/lock_client.h"


namespace phxqueue_phxrpc {

namespace scheduler {


Scheduler::Scheduler(const phxqueue::scheduler::SchedulerOption &opt)
        : phxqueue::scheduler::Scheduler(opt) {}

Scheduler::~Scheduler() {}

phxqueue::comm::RetCode
Scheduler::GetLockInfo(const phxqueue::comm::proto::GetLockInfoRequest &req,
                       phxqueue::comm::proto::GetLockInfoResponse &resp) {
    LockClient lock_client;
    auto ret(lock_client.ProtoGetLockInfo(req, resp));
    if (phxqueue::comm::RetCode::RET_OK != ret) {
        QLErr("ProtoGetLockInfo ret %d", phxqueue::comm::as_integer(ret));
    }
    return ret;
}

phxqueue::comm::RetCode
Scheduler::AcquireLock(const phxqueue::comm::proto::AcquireLockRequest &req,
                       phxqueue::comm::proto::AcquireLockResponse &resp) {
    LockClient lock_client;
    auto ret(lock_client.ProtoAcquireLock(req, resp));
    if (phxqueue::comm::RetCode::RET_OK != ret) {
        QLErr("ProtoAcquireLock ret %d", phxqueue::comm::as_integer(ret));
    }
    return ret;
}


}  // namespace scheduler

}  // namespace phxqueue_phxrpc

