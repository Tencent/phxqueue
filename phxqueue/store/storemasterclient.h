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

namespace store {


template <typename Req, typename Resp>
class StoreMasterClient : public comm::MasterClient<Req, Resp> {
  public:
    StoreMasterClient() {}
    virtual ~StoreMasterClient() {}
  protected:
    virtual std::string GetRouteKeyByReq(const Req &req);
    virtual comm::RetCode GetCandidateAddrs(const Req &req, std::vector<comm::proto::Addr> &addrs);
};


template <typename Req, typename Resp>
std::string StoreMasterClient<Req, Resp>::GetRouteKeyByReq(const Req &req) {
    return comm::GetRouteKey(req.topic_id(), req.store_id(), req.queue_id());
}


template <typename Req, typename Resp>
comm::RetCode StoreMasterClient<Req, Resp>::GetCandidateAddrs(const Req &req,
        std::vector<comm::proto::Addr> &addrs) {
    addrs.clear();

    comm::RetCode ret;

    std::shared_ptr<const config::StoreConfig> store_config;
    if (comm::RetCode::RET_OK !=
        (ret = config::GlobalConfig::GetThreadInstance()->
         GetStoreConfig(req.topic_id(), store_config))) {
        //QLErr("GetStoreConfig ret %d topic_id %d", as_integer(ret), req.topic_id());
        return ret;
    }

    std::shared_ptr<const config::proto::Store> store;
    if (comm::RetCode::RET_OK != (ret = store_config->GetStoreByStoreID(req.store_id(), store))) {
        //QLErr("GetStoreByStoreID ret %d store_id %d", as_integer(ret), req.store_id());
        return ret;
    }

    for (int i{0}; i < store->addrs_size(); ++i) {
        addrs.push_back(store->addrs(i));
    }

    return comm::RetCode::RET_OK;
}


}  // namespace store

}  // namespace phxqueue

