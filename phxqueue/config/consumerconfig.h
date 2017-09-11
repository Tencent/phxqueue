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

#include "phxqueue/config/baseconfig.h"
#include "phxqueue/config/proto/consumerconfig.pb.h"


namespace phxqueue {

namespace config {


class ConsumerConfig : public BaseConfig<proto::ConsumerConfig> {
  public:
    ConsumerConfig();

    virtual ~ConsumerConfig();

    comm::RetCode GetAllConsumer(std::vector<std::shared_ptr<const proto::Consumer>> &consumers) const;

    comm::RetCode GetConsumerByAddr(const comm::proto::Addr &addr, std::shared_ptr<const proto::Consumer> &consumer) const;

  protected:
    virtual comm::RetCode ReadConfig(proto::ConsumerConfig &proto);

    comm::RetCode Rebuild() override;

  private:
    class ConsumerConfigImpl;
    std::unique_ptr<ConsumerConfigImpl> impl_;
};


}  // namespace config

}  // namespace phxqueue

