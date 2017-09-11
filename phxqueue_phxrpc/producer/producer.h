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


namespace phxqueue_phxrpc {

namespace producer {


class Producer : public phxqueue::producer::Producer {
  public:
    Producer(const phxqueue::producer::ProducerOption &opt);
    virtual ~Producer();

    virtual void CompressBuffer(const std::string &buffer, std::string &compressed_buffer,
                                int &buffer_type);

  protected:
    virtual phxqueue::comm::RetCode Add(const phxqueue::comm::proto::AddRequest &req,
                                        phxqueue::comm::proto::AddResponse &resp);
};


}  // namespace producer

}  // namespace phxqueue_phxrpc

