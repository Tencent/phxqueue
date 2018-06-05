/*
Tencent is pleased to support the open source community by making PhxQueue available.
Copyright (C) 2017 THL A29 Limited, a Tencent company. All rights reserved.
Licensed under the BSD 3-Clause License (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at

<https://opensource.org/licenses/BSD-3-Clause>

Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.
*/



#include "phxrpc_scheduler.h"

#include "phxqueue_phxrpc/app/lock/lock_client.h"


namespace phxqueue_phxrpc {

namespace scheduler {


PhxRpcScheduler::PhxRpcScheduler(const phxqueue::SchedulerOption &opt)
        : phxqueue::Scheduler(opt) {}

PhxRpcScheduler::~PhxRpcScheduler() {}

phxqueue::RetCode
PhxRpcScheduler::GetLockInfo(const phxqueue_proto::GetLockInfoRequest &req,
                             phxqueue_proto::GetLockInfoResponse &resp) {
    LockClient client;
    return client.ProtoGetLockInfo(req, resp);
}

phxqueue::RetCode
PhxRpcScheduler::AcquireLock(const phxqueue_proto::AcquireLockRequest &req,
                             phxqueue_proto::AcquireLockResponse &resp) {
    LockClient client;
    return client.ProtoAcquireLock(req, resp);
}


}

}

