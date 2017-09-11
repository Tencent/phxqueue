/*
Tencent is pleased to support the open source community by making PhxQueue available.
Copyright (C) 2017 THL A29 Limited, a Tencent company. All rights reserved.
Licensed under the BSD 3-Clause License (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at

<https://opensource.org/licenses/BSD-3-Clause>

Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.
*/



#pragma once

#include "phxqueue/lock/lock.h"


namespace phxqueue {

namespace lock {


class KeepMasterThread {
  public:
    KeepMasterThread(Lock *lock);
    virtual ~KeepMasterThread();

    void Run();
    void Stop();

  private:
    void DoRun();
    comm::RetCode InitMasterRate();
    comm::RetCode AdjustMasterRate();
    comm::RetCode KeepMaster();
    //comm::RetCode GetIdxInGroupAndGroupSize(int &idx_in_group, int &group_size);
    comm::RetCode ProposeMaster(const int paxos_group_id, const comm::proto::Addr addr);
    comm::RetCode UpdatePaxosArgs();

    class KeepMasterThreadImpl;
    std::unique_ptr<KeepMasterThreadImpl> impl_;
};


}  // namespace lock

}  // namespace phxqueue

