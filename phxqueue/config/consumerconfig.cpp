/*
Tencent is pleased to support the open source community by making PhxQueue available.
Copyright (C) 2017 THL A29 Limited, a Tencent company. All rights reserved.
Licensed under the BSD 3-Clause License (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at

<https://opensource.org/licenses/BSD-3-Clause>

Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.
*/



#include "phxqueue/config/consumerconfig.h"

#include "phxqueue/comm.h"


namespace phxqueue {

namespace config {


using namespace std;


class ConsumerConfig::ConsumerConfigImpl {
  public:
    ConsumerConfigImpl() {}
    virtual ~ConsumerConfigImpl() {}

    map<uint64_t, shared_ptr<proto::Consumer>> addr2consumer;
};

ConsumerConfig::ConsumerConfig() : impl_(new ConsumerConfigImpl()) {
    assert(impl_);
}

ConsumerConfig::~ConsumerConfig() {}

comm::RetCode ConsumerConfig::ReadConfig(proto::ConsumerConfig &proto) {
    // sample
    proto.Clear();

    proto::Consumer *consumer = nullptr;
    comm::proto::Addr *addr = nullptr;

    consumer = proto.add_consumers();
    consumer->set_scale(100);
    consumer->add_consumer_group_ids(1);
    consumer->add_consumer_group_ids(2);
    addr = consumer->mutable_addr();
    addr->set_ip("127.0.0.1");
    addr->set_port(8001);

    consumer = proto.add_consumers();
    consumer->set_scale(100);
    consumer->add_consumer_group_ids(1);
    consumer->add_consumer_group_ids(2);
    addr = consumer->mutable_addr();
    addr->set_ip("127.0.0.1");
    addr->set_port(8002);

    consumer = proto.add_consumers();
    consumer->set_scale(100);
    consumer->add_consumer_group_ids(1);
    consumer->add_consumer_group_ids(2);
    addr = consumer->mutable_addr();
    addr->set_ip("127.0.0.1");
    addr->set_port(8003);

    return comm::RetCode::RET_OK;
}


comm::RetCode ConsumerConfig::Rebuild() {
    bool need_check = NeedCheck();

    impl_->addr2consumer.clear();

    auto &&proto = GetProto();

    for (int i{0}; i < proto.consumers_size(); ++i) {
        const auto &consumer(proto.consumers(i));
        if (need_check) PHX_ASSERT(impl_->addr2consumer.end() == impl_->addr2consumer.find(comm::utils::EncodeAddr(consumer.addr())), ==, true);
        impl_->addr2consumer.emplace(comm::utils::EncodeAddr(consumer.addr()), make_shared<proto::Consumer>(consumer));
    }
    return comm::RetCode::RET_OK;
}

comm::RetCode ConsumerConfig::GetAllConsumer(std::vector<shared_ptr<const proto::Consumer>> &consumers) const {
    for (auto &&it : impl_->addr2consumer) {
        consumers.push_back(it.second);
    }
    return comm::RetCode::RET_OK;
}

comm::RetCode ConsumerConfig::GetConsumerByAddr(const comm::proto::Addr &addr, shared_ptr<const proto::Consumer> &consumer) const {
    auto &&encoded_addr = comm::utils::EncodeAddr(addr);
    auto &&it = impl_->addr2consumer.find(encoded_addr);
    if (impl_->addr2consumer.end() == it) return comm::RetCode::RET_ERR_RANGE_ADDR;
    consumer = it->second;
    return comm::RetCode::RET_OK;
}


}  // namespace config

}  // namespace phxqueue


//gzrd_Lib_CPP_Version_ID--start
#ifndef GZRD_SVN_ATTR
#define GZRD_SVN_ATTR "0"
#endif
static char gzrd_Lib_CPP_Version_ID[] __attribute__((used))="$HeadURL$ $Id$ " GZRD_SVN_ATTR "__file__";
// gzrd_Lib_CPP_Version_ID--end

