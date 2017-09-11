/*
Tencent is pleased to support the open source community by making PhxQueue available.
Copyright (C) 2017 THL A29 Limited, a Tencent company. All rights reserved.
Licensed under the BSD 3-Clause License (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at

<https://opensource.org/licenses/BSD-3-Clause>

Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.
*/



#pragma once

#include <memory>

#include "phxqueue/comm.h"
#include "phxqueue/config.h"


namespace phxqueue {

namespace lock {


template <typename Req, typename Resp>
class LockMasterClient : public comm::MasterClient<Req, Resp> {
  public:
    LockMasterClient() = default;
    virtual ~LockMasterClient() override = default;

  protected:
    virtual std::string GetRouteKeyByReq(const Req &req) override;
    virtual comm::RetCode GetCandidateAddrs(const Req &req,
                                            std::vector<comm::proto::Addr> &addrs) override;
};

template <typename Req, typename Resp>
std::string LockMasterClient<Req, Resp>::GetRouteKeyByReq(const Req &req) {
    return comm::GetRouteKey(req.topic_id(), req.lock_id(), req.lock_key());
}

template <>
inline std::string LockMasterClient<comm::proto::AcquireLockRequest,
       comm::proto::AcquireLockResponse>::GetRouteKeyByReq(
       const comm::proto::AcquireLockRequest &req) {
    return comm::GetRouteKey(req.topic_id(), req.lock_id(), req.lock_info().lock_key());
}

template <typename Req, typename Resp>
comm::RetCode LockMasterClient<Req, Resp>::GetCandidateAddrs(const Req &req,
                                                             std::vector<comm::proto::Addr> &addrs) {
    addrs.clear();

    std::shared_ptr<const config::LockConfig> lock_config;
    comm::RetCode ret{config::GlobalConfig::GetThreadInstance()->
            GetLockConfig(req.topic_id(), lock_config)};
    if (comm::RetCode::RET_OK != ret) {
        QLErr("GetLockConfig ret %d topic_id %d", as_integer(ret), req.topic_id());

        return ret;
    }

    std::shared_ptr<const config::proto::Lock> lock;
    ret = lock_config->GetLockByLockID(req.lock_id(), lock);
    if (comm::RetCode::RET_OK != ret) {
        QLErr("GetLockByLockID ret %d topic_id %d lock_id %d",
              as_integer(ret), req.topic_id(), req.lock_id());

        return ret;
    }

    std::string addrs_str;
    for (int i{0}; lock->addrs_size() > i; ++i) {
        addrs.push_back(lock->addrs(i));
        addrs_str += lock->addrs(i).ip() + ";";
    }
    QLVerb("addrs {%s}", addrs_str.c_str());

    return comm::RetCode::RET_OK;
}


}  // namespace lock

}  // namespace phxqueue

