/*
Tencent is pleased to support the open source community by making PhxQueue available.
Copyright (C) 2017 THL A29 Limited, a Tencent company. All rights reserved.
Licensed under the BSD 3-Clause License (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at

<https://opensource.org/licenses/BSD-3-Clause>

Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.
*/



#pragma once

#include <memory>

#include "phxqueue/producer.h"


namespace phxqueue {

namespace test {


class SimpleProducer : public producer::Producer {
  public:
    SimpleProducer(const producer::ProducerOption &opt) : producer::Producer(opt) {}
    virtual ~SimpleProducer() override = default;

    virtual void CompressBuffer(const std::string &buffer, std::string &compress_buffer,
                                int &buffer_type) override;

  protected:
    virtual comm::RetCode Add(const comm::proto::AddRequest &req,
                              comm::proto::AddResponse &resp) override;

};


}  // namespace test

}  // namespace phxqueue

