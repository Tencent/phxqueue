/*
Tencent is pleased to support the open source community by making PhxQueue available.
Copyright (C) 2017 THL A29 Limited, a Tencent company. All rights reserved.
Licensed under the BSD 3-Clause License (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at

<https://opensource.org/licenses/BSD-3-Clause>

Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.
*/



#include "phxqueue/test/simpleconsumer.h"

#include <iostream>
#include <unistd.h>

#include "phxqueue/comm.h"


namespace phxqueue {

namespace test {


using namespace std;


comm::RetCode SimpleConsumer::Get(const comm::proto::GetRequest &req,
                                  comm::proto::GetResponse &resp) {
    sleep(1);

    QLInfo("Get topic_id %d store_id %d queue_id %d", req.topic_id(), req.store_id(), req.queue_id());

    comm::proto::QItem item;
    comm::proto::Meta &meta = *item.mutable_meta();

    meta.set_topic_id(1000);
    meta.set_handle_id(1);

    resp.set_prev_cursor_id(1);
    resp.set_next_cursor_id(1);

    item.mutable_meta()->set_uin(111);
    resp.add_items()->CopyFrom(item);
    item.mutable_meta()->set_uin(222);
    resp.add_items()->CopyFrom(item);

    return comm::RetCode::RET_OK;
}

comm::RetCode SimpleConsumer::Add(comm::proto::AddRequest &req,
                                  comm::proto::AddResponse &resp) {
    return comm::RetCode::RET_OK;
}

comm::RetCode SimpleConsumer::UncompressBuffer(const std::string &buffer, const int buffer_type,
                                               std::string &decoded_buffer) {
    decoded_buffer = buffer;
    return comm::RetCode::RET_OK;
}

void SimpleConsumer::RestoreUserCookies(const comm::proto::Cookies &user_cookies) {
}

void SimpleConsumer::CompressBuffer(const std::string &buffer, std::string &compress_buffer,
                                    const int buffer_type) {
    compress_buffer = buffer;
}


comm::RetCode SimpleConsumer::GetAddrScale(const comm::proto::GetAddrScaleRequest &req,
                                           comm::proto::GetAddrScaleResponse &resp) {
    // return default
    comm::RetCode ret;

    shared_ptr<const config::ConsumerConfig> consumer_config;
    if (comm::RetCode::RET_OK != (ret = config::GlobalConfig::GetThreadInstance()->
                                  GetConsumerConfig(GetTopicID(), consumer_config))) {
        QLErr("GetConsumerConfig ret %d", as_integer(ret));
        return ret;
    }

    for (int i{0}; i < consumer_config->GetProto().consumers_size(); ++i) {
        auto &&consumer = consumer_config->GetProto().consumers(i);
        auto &&addr_scale = resp.add_addr_scales();
        addr_scale->mutable_addr()->CopyFrom(consumer.addr());
        addr_scale->set_scale(consumer.scale());
    }

    return comm::RetCode::RET_OK;
}

comm::RetCode SimpleConsumer::GetQueueByAddrScale(const std::vector<consumer::Queue_t> &queues,
                                                  const consumer::AddrScales &addr_scales,
                                                  std::set<size_t> &queue_idxs) {
    queue_idxs.clear();

    auto opt = GetConsumerOption();

    comm::proto::Addr addr;
    addr.set_ip(opt->ip);
    addr.set_port(opt->port);
    addr.set_paxos_port(opt->paxos_port);

    size_t i;
    for (i = 0; i < addr_scales.size(); ++i) {
        auto &&addr_scale = addr_scales[i];
        if (addr.ip() == addr_scale.addr().ip() &&
            addr.port() == addr_scale.addr().port() &&
            addr.paxos_port() == addr_scale.addr().paxos_port()) {
            break;
        }
    }
    if (i == addr_scales.size()) {
        QLErr("local addr not found");
    }

    for (size_t j{0}; j < queues.size(); ++j) {
        auto &&queue = queues[j];
        if (i == (queue.queue_id % addr_scales.size())) {
            queue_idxs.insert(j);
        }
    }

    return comm::RetCode::RET_OK;
}

comm::RetCode SimpleConsumer::GetLockInfo(const comm::proto::GetLockInfoRequest &req,
                                          comm::proto::GetLockInfoResponse &resp) {
    return comm::RetCode::RET_ERR_NO_IMPL;
}

comm::RetCode SimpleConsumer::AcquireLock(const comm::proto::AcquireLockRequest &req,
                                          comm::proto::AcquireLockResponse &resp) {
    return comm::RetCode::RET_ERR_NO_IMPL;
}

void SimpleConsumer::BeforeLock(const comm::proto::ConsumerContext &cc) {
    QLInfo("BeforeLock cc consumer_group_id %d store_id %d queue_id %d",
           cc.consumer_group_id(), cc.store_id(), cc.queue_id());
}

void SimpleConsumer::AfterLock(const comm::proto::ConsumerContext &cc) {
    QLInfo("AfterLock cc consumer_group_id %d store_id %d queue_id %d",
           cc.consumer_group_id(), cc.store_id(), cc.queue_id());
}


}  // namespace test

}  // namespace phxqueue

