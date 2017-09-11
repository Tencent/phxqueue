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
    consumer->add_sub_ids(1);
    consumer->add_sub_ids(2);
    addr = consumer->mutable_addr();
    addr->set_ip("127.0.0.1");
    addr->set_port(8001);

    consumer = proto.add_consumers();
    consumer->set_scale(100);
    consumer->add_sub_ids(1);
    consumer->add_sub_ids(2);
    addr = consumer->mutable_addr();
    addr->set_ip("127.0.0.1");
    addr->set_port(8002);

    consumer = proto.add_consumers();
    consumer->set_scale(100);
    consumer->add_sub_ids(1);
    consumer->add_sub_ids(2);
    addr = consumer->mutable_addr();
    addr->set_ip("127.0.0.1");
    addr->set_port(8003);

    return comm::RetCode::RET_OK;
}


comm::RetCode ConsumerConfig::Rebuild() {
    impl_->addr2consumer.clear();

    auto &&proto = GetProto();

    for (int i{0}; i < proto.consumers_size(); ++i) {
        const auto &consumer(proto.consumers(i));
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

