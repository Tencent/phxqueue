/*
Tencent is pleased to support the open source community by making PhxQueue available.
Copyright (C) 2017 THL A29 Limited, a Tencent company. All rights reserved.
Licensed under the BSD 3-Clause License (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at

<https://opensource.org/licenses/BSD-3-Clause>

Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.
*/



#pragma once

#include <memory>
#include <unistd.h>

#include "phxqueue/consumer/consumer.h"


namespace phxqueue {

namespace consumer {


class FreqMan {
  public:
    FreqMan();
    virtual ~FreqMan();
    comm::RetCode Init(const int topic_id, Consumer *consumer);
    void Run();
    void UpdateConsumeStat(const int vpid, const comm::proto::ConsumerContext &cc,
                           const std::vector<std::shared_ptr<comm::proto::QItem>> &items);
    void Judge(const int vpid, bool &need_block, bool &need_freqlimit,
               int &nrefill, int &sleep_ms_per_get_recommand);

  private:
    void ClearAllConsumeStat();
    comm::RetCode UpdateLimitInfo();

    class FreqManImpl;
    std::unique_ptr<FreqManImpl> impl_;
};


}  // namespace consumer

}  // namespace phxqueue

