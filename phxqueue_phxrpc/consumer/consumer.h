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


namespace phxqueue_phxrpc {

namespace consumer {


class Consumer : public phxqueue::consumer::Consumer {
  public:
    Consumer(const phxqueue::consumer::ConsumerOption &opt);
    virtual ~Consumer() override;

    virtual phxqueue::comm::RetCode
    UncompressBuffer(const std::string &buffer, const int buffer_type,
                     std::string &uncompressed_buffer) override;
    virtual void CompressBuffer(const std::string &buffer,
                                std::string &compress_buffer, const int buffer_type) override;
    virtual phxqueue::comm::RetCode
    Get(const phxqueue::comm::proto::GetRequest &req, phxqueue::comm::proto::GetResponse &resp) override;
    virtual phxqueue::comm::RetCode
    Add(phxqueue::comm::proto::AddRequest &req, phxqueue::comm::proto::AddResponse &resp) override;
    virtual phxqueue::comm::RetCode
    GetAddrScale(const phxqueue::comm::proto::GetAddrScaleRequest &req,
                 phxqueue::comm::proto::GetAddrScaleResponse &resp) override;
    virtual phxqueue::comm::RetCode
    GetLockInfo(const phxqueue::comm::proto::GetLockInfoRequest &req,
                phxqueue::comm::proto::GetLockInfoResponse &resp) override;
    virtual phxqueue::comm::RetCode
    AcquireLock(const phxqueue::comm::proto::AcquireLockRequest &req,
                phxqueue::comm::proto::AcquireLockResponse &resp) override;

  private:
    virtual void RestoreUserCookies(const phxqueue::comm::proto::Cookies &user_cookies) {}
};


}  // namespace consumer

}  // namespace phxqueue_phxrpc

