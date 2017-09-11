/*
Tencent is pleased to support the open source community by making PhxQueue available.
Copyright (C) 2017 THL A29 Limited, a Tencent company. All rights reserved.
Licensed under the BSD 3-Clause License (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at

<https://opensource.org/licenses/BSD-3-Clause>

Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.
*/



#include "phxqueue/test/simpleproducer.h"

#include <iostream>
#include <memory>

#include "phxqueue/comm.h"
#include "phxqueue/config.h"


namespace phxqueue {

namespace test {


using namespace std;


void SimpleProducer::CompressBuffer(const string &buffer, string &compress_buffer,
                                    int &buffer_type) {
    QLVerb("CompressBuffer");
    buffer_type = 0;
    compress_buffer = buffer;
}

comm::RetCode SimpleProducer::Add(const comm::proto::AddRequest &req,
                                  comm::proto::AddResponse &resp) {
    QLVerb("Add topic_id %d store_id %d queue_id %d",
           req.topic_id(), req.store_id(), req.queue_id());

    comm::RetCode ret;

    shared_ptr<const config::StoreConfig> store_config;
    if (comm::RetCode::RET_OK != (ret = config::GlobalConfig::GetThreadInstance()->
                                  GetStoreConfig(req.topic_id(), store_config))) {
        QLErr("GetStoreConfig ret %d topic_id %d", as_integer(ret), req.topic_id());
        return ret;
    }

    shared_ptr<const config::proto::Store> store;
    if (comm::RetCode::RET_OK != (ret = store_config->GetStoreByStoreID(req.store_id(), store))) {
        QLErr("GetStoreByStoreID ret %d store_id %d", as_integer(ret), req.store_id());
        return ret;
    }

    assert(store->addrs_size() >= 2);
    comm::proto::Addr master_addr = store->addrs(1);

    if (req.master_addr().ip() == master_addr.ip() &&
        req.master_addr().port() == master_addr.port() &&
        req.master_addr().paxos_port() == master_addr.paxos_port()) {

        resp.set_cursor_id(1);
        return comm::RetCode::RET_OK;
    }
    resp.set_cursor_id(-1);
    resp.mutable_redirect_addr()->CopyFrom(master_addr);
    return comm::RetCode::RET_ERR_NOT_MASTER;
}


}  // namespace test

}  // namespace phxqueue

