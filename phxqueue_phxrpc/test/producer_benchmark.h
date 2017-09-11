/*
Tencent is pleased to support the open source community by making PhxQueue available.
Copyright (C) 2017 THL A29 Limited, a Tencent company. All rights reserved.
Licensed under the BSD 3-Clause License (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at

<https://opensource.org/licenses/BSD-3-Clause>

Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.
*/



#pragma once

#include <memory>
#include <string>

#include "phxqueue/comm.h"
#include "phxqueue_phxrpc/producer.h"


namespace phxqueue_phxrpc {

namespace test {


class ProducerBenchMark : public phxqueue::comm::utils::BenchMark {
  public:
    ProducerBenchMark(const int qps, const int nthread, const int nroutine,
                      const int buf_size, const int ndaemon_batch_thread = 0);
    virtual ~ProducerBenchMark() override = default;
    int TaskFunc(const int vtid);

  private:
    std::string buf_;
    std::unique_ptr<phxqueue::producer::Producer> producer_;
};


}  // namespace test

}  // namespace phxqueue_phxrpc

