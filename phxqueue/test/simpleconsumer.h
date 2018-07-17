/*
Tencent is pleased to support the open source community by making PhxQueue available.
Copyright (C) 2017 THL A29 Limited, a Tencent company. All rights reserved.
Licensed under the BSD 3-Clause License (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at

<https://opensource.org/licenses/BSD-3-Clause>

Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.
*/



#pragma once

#include <memory>

#include "phxqueue/consumer.h"


namespace phxqueue {

namespace test {


class SimpleConsumer : public consumer::Consumer {
  public:
    SimpleConsumer(const consumer::ConsumerOption &opt) : consumer::Consumer(opt) {}
    virtual ~SimpleConsumer() override = default;

    virtual comm::RetCode Get(const comm::proto::GetRequest &req,
                              comm::proto::GetResponse &resp) override;
    virtual comm::RetCode Add(comm::proto::AddRequest &req,
                              comm::proto::AddResponse &resp) override;

    virtual comm::RetCode UncompressBuffer(const std::string &buffer,
                                           const int buffer_type,
                                           std::string &decoded_buffer) override;
    virtual void RestoreUserCookies(const comm::proto::Cookies &user_cookies) override;
    virtual void CompressBuffer(const std::string &buffer, std::string &compress_buffer,
                                const int buffer_type) override;
    virtual comm::RetCode GetAddrScale(const comm::proto::GetAddrScaleRequest &req,
                                       comm::proto::GetAddrScaleResponse &resp) override;
    virtual comm::RetCode GetQueueByAddrScale(const std::vector<consumer::Queue_t> &queues,
                                              const consumer::AddrScales &addr_scales,
                                              std::set<size_t> &queue_idxs) override;
    virtual comm::RetCode GetLockInfo(const comm::proto::GetLockInfoRequest &req,
                                      comm::proto::GetLockInfoResponse &resp) override;
    virtual comm::RetCode AcquireLock(const comm::proto::AcquireLockRequest &req,
                                      comm::proto::AcquireLockResponse &resp) override;

    virtual void BeforeLock(const comm::proto::ConsumerContext &cc) override;
    virtual void AfterLock(const comm::proto::ConsumerContext &cc) override;
};


}  // namespace test

}  // namespace phxqueue

