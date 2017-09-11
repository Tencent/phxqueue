/*
Tencent is pleased to support the open source community by making PhxQueue available.
Copyright (C) 2017 THL A29 Limited, a Tencent company. All rights reserved.
Licensed under the BSD 3-Clause License (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at

<https://opensource.org/licenses/BSD-3-Clause>

Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.
*/



#pragma once

#include <memory>

#include "phxqueue/scheduler.h"


namespace phxqueue {

namespace test {


class SimpleScheduler : public scheduler::Scheduler {
  public:
    SimpleScheduler(const scheduler::SchedulerOption &opt) : scheduler::Scheduler(opt) {}
    virtual ~SimpleScheduler() override = default;

  protected:
    virtual comm::RetCode GetLockInfo(const comm::proto::GetLockInfoRequest &req,
                                      comm::proto::GetLockInfoResponse &resp) override;
    virtual comm::RetCode AcquireLock(const comm::proto::AcquireLockRequest &req,
                                      comm::proto::AcquireLockResponse &resp) override;
};


}  // namespace test

}  // namespace phxqueue

