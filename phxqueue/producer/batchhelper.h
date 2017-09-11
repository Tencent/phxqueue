/*
Tencent is pleased to support the open source community by making PhxQueue available.
Copyright (C) 2017 THL A29 Limited, a Tencent company. All rights reserved.
Licensed under the BSD 3-Clause License (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at

<https://opensource.org/licenses/BSD-3-Clause>

Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.
*/



#pragma once

#include <memory>
#include <set>
#include <vector>

#include "phxqueue/comm.h"
#include "phxqueue/producer/producer.h"

namespace phxqueue {

namespace producer {

class ProcessCtx_t;

class BatchHelper {
  public:
    BatchHelper(producer::Producer *producer);
    ~BatchHelper();

    void Init();
    void Run();
    void Stop();

    comm::RetCode BatchRawAdd(const comm::proto::AddRequest &req);

    void DispatchBatchTask(int vtid);
    void Process(ProcessCtx_t *ctx);

  protected:
    void DaemonThreadRun(int vtid);

  private:
    class BatchHelperImpl;
    std::unique_ptr<BatchHelperImpl> impl_;
};

}  // namespace producer

}  // namespace phxqueue

