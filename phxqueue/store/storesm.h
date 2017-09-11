/*
Tencent is pleased to support the open source community by making PhxQueue available.
Copyright (C) 2017 THL A29 Limited, a Tencent company. All rights reserved.
Licensed under the BSD 3-Clause License (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at

<https://opensource.org/licenses/BSD-3-Clause>

Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.
*/



#pragma once

#include <cstdio>
#include <memory>

#include "phxqueue/comm.h"
#include "phxpaxos/options.h"

#include "phxqueue/store/checkpointstat.h"
#include "phxqueue/store/store.h"


namespace phxqueue {

namespace store {


class StoreSM : public phxpaxos::StateMachine {
  public:
    StoreSM(Store *const store);
    virtual ~StoreSM();

    static constexpr int ID = 1;

    virtual bool Execute(const int paxos_group_id, const uint64_t instance_id,
                         const std::string &paxos_value, phxpaxos::SMCtx *ctx) override;

    virtual const int SMID() const override {
        return ID;
    }

    virtual bool ExecuteForCheckpoint(const int paxos_group_id, const uint64_t instance_id,
                                      const std::string &paxos_value) override {
        return true;
    }

    virtual const uint64_t GetCheckpointInstanceID(const int paxos_group_id) const override;

    virtual int GetCheckpointState(const int paxos_group_id, std::string &dir_path,
                                   std::vector<std::string> &file_list) override;

    virtual int LoadCheckpointState(const int paxos_group_id, const std::string &tmp_dir_path,
                                    const std::vector<std::string> &vecFileList,
                                    const uint64_t cp) override;

    virtual int LockCheckpointState() override {
        return 0;
    }

    virtual void UnLockCheckpointState() override {}

  private:
    class StoreSMImpl;
    std::unique_ptr<StoreSMImpl> impl_;
};


} // namespace store

} // namespace phxqueue

