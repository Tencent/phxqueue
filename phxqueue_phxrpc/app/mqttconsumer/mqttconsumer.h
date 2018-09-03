/*
Tencent is pleased to support the open source community by making PhxQueue available.
Copyright (C) 2017 THL A29 Limited, a Tencent company. All rights reserved.
Licensed under the BSD 3-Clause License (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at

<https://opensource.org/licenses/BSD-3-Clause>

Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.
*/



#pragma once

#include "phxqueue_phxrpc/consumer.h"


namespace phxqueue_phxrpc {

namespace mqttconsumer {


class MqttConsumer : public phxqueue_phxrpc::consumer::Consumer {
  public:
    MqttConsumer(const phxqueue::consumer::ConsumerOption &opt);
    virtual ~MqttConsumer() override;

  protected:
    virtual void CustomGetRequest(const phxqueue::comm::proto::ConsumerContext &cc,
                                  const phxqueue::comm::proto::GetRequest &req,
                                  uint64_t &pre_cursor_id, uint64_t &next_cursor_id,
                                  int &limit) override;
};


}  // namespace mqttconsumer

}  // namespace phxqueue_phxrpc

