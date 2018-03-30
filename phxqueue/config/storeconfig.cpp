/*
Tencent is pleased to support the open source community by making PhxQueue available.
Copyright (C) 2017 THL A29 Limited, a Tencent company. All rights reserved.
Licensed under the BSD 3-Clause License (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at

<https://opensource.org/licenses/BSD-3-Clause>

Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.
*/



#include "phxqueue/config/storeconfig.h"

#include "phxqueue/comm.h"


namespace phxqueue {

namespace config {


using namespace std;


class StoreConfig::StoreConfigImpl {
  public:
    StoreConfigImpl() {}
    virtual ~StoreConfigImpl() {}

    map<int, shared_ptr<proto::Store>> store_id2store;
    map<uint64_t, int> addr2store_id;
};

StoreConfig::StoreConfig() : impl_(new StoreConfigImpl()){
    assert(impl_);
}

StoreConfig::~StoreConfig() {}

comm::RetCode StoreConfig::ReadConfig(proto::StoreConfig &proto) {
    QLVerb("start");

    // sample
    proto.Clear();

    proto::Store *store = nullptr;
    comm::proto::Addr *addr = nullptr;

    // store 1
    {
        store = proto.add_stores();
        store->set_store_id(1);
        store->set_scale(100);
        store->add_pub_ids(1);
        store->add_pub_ids(2);

        addr = store->add_addrs();
        addr->set_ip("127.0.0.1");
        addr->set_port(5100);
        addr->set_paxos_port(5101);

        addr = store->add_addrs();
        addr->set_ip("127.0.0.1");
        addr->set_port(5200);
        addr->set_paxos_port(5201);

        addr = store->add_addrs();
        addr->set_ip("127.0.0.1");
        addr->set_port(5300);
        addr->set_paxos_port(5301);
    }

    return comm::RetCode::RET_OK;
}


comm::RetCode StoreConfig::Rebuild() {
    bool need_check = NeedCheck();

    QLVerb("start");

    impl_->store_id2store.clear();
    impl_->addr2store_id.clear();

    auto &&proto = GetProto();

    for (int i{0}; i < proto.stores_size(); ++i) {
        const auto &store(proto.stores(i));
        if (!store.store_id()) continue;
        if (need_check) PHX_ASSERT(impl_->store_id2store.end() == impl_->store_id2store.find(store.store_id()), ==, true);
        impl_->store_id2store.emplace(store.store_id(), make_shared<proto::Store>(store));

        for (int j{0}; j < store.addrs_size(); ++j) {
            auto &&addr = store.addrs(j);
            if (need_check) PHX_ASSERT(impl_->addr2store_id.end() == impl_->addr2store_id.find(comm::utils::EncodeAddr(addr)), ==, true);
            impl_->addr2store_id.emplace(comm::utils::EncodeAddr(addr), store.store_id());
            QLVerb("add addr(%s:%d:%d) store_id %d", addr.ip().c_str(), addr.port(), addr.paxos_port(), store.store_id());
        }
    }

    return comm::RetCode::RET_OK;
}


comm::RetCode StoreConfig::GetAllStore(std::vector<shared_ptr<const proto::Store>> &stores) const {
    for (auto &&it : impl_->store_id2store) {
        stores.push_back(it.second);
    }
    return comm::RetCode::RET_OK;
}

comm::RetCode StoreConfig::GetAllStoreID(std::set<int> &store_ids) const {
    for (auto &&it : impl_->store_id2store) {
        store_ids.insert(it.first);
    }
    return comm::RetCode::RET_OK;
}

comm::RetCode StoreConfig::GetStoreByStoreID(const int store_id, shared_ptr<const proto::Store> &store) const {
    auto it(impl_->store_id2store.find(store_id));
    if (it == impl_->store_id2store.end()) return comm::RetCode::RET_ERR_RANGE_STORE;
    store = it->second;
    return comm::RetCode::RET_OK;
}

comm::RetCode StoreConfig::GetStoreIDByAddr(const comm::proto::Addr &addr, int &store_id) const {
    auto &&encoded_addr = comm::utils::EncodeAddr(addr);
    auto &&it = impl_->addr2store_id.find(encoded_addr);
    if (impl_->addr2store_id.end() == it) return comm::RetCode::RET_ERR_RANGE_ADDR;
    store_id = it->second;
    return comm::RetCode::RET_OK;
}

comm::RetCode StoreConfig::GetStoreByAddr(const comm::proto::Addr &addr, std::shared_ptr<const proto::Store> &store) const {
    comm::RetCode ret;

    int store_id;
    if (comm::RetCode::RET_OK != (ret = GetStoreIDByAddr(addr, store_id))) return ret;
    return GetStoreByStoreID(store_id, store);
}


}  // namespace config

}  // namespace phxqueue


//gzrd_Lib_CPP_Version_ID--start
#ifndef GZRD_SVN_ATTR
#define GZRD_SVN_ATTR "0"
#endif
static char gzrd_Lib_CPP_Version_ID[] __attribute__((used))="$HeadURL$ $Id$ " GZRD_SVN_ATTR "__file__";
// gzrd_Lib_CPP_Version_ID--end

