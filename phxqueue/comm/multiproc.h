/*
Tencent is pleased to support the open source community by making PhxQueue available.
Copyright (C) 2017 THL A29 Limited, a Tencent company. All rights reserved.
Licensed under the BSD 3-Clause License (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at

<https://opensource.org/licenses/BSD-3-Clause>

Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.
*/



#pragma once

#include <cinttypes>
#include <pthread.h>
#include <vector>

#include "phxqueue/comm/errdef.h"


namespace phxqueue {

namespace comm {


class MultiProc {
  public:
    MultiProc() {}
    virtual ~MultiProc() {}

    // watch and refork the child process if killed/aborted unexpectedly
    void ForkAndRun(const int procs);

  protected:
    virtual void ChildRun(const int vpid) = 0;

  private:
    std::vector<pid_t> children_;
};


}  // namespace comm

}  // namespace phxqueue

