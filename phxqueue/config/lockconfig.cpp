/*
Tencent is pleased to support the open source community by making PhxQueue available.
Copyright (C) 2017 THL A29 Limited, a Tencent company. All rights reserved.
Licensed under the BSD 3-Clause License (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at

<https://opensource.org/licenses/BSD-3-Clause>

Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.
*/



#include "phxqueue/config/lockconfig.h"

#include "phxqueue/comm.h"


namespace phxqueue {

namespace config {


using namespace std;


class LockConfig::LockConfigImpl {
  public:
    LockConfigImpl() {}
    virtual ~LockConfigImpl() {}

    map<int, shared_ptr<proto::Lock>> lock_id2lock;
    map<uint64_t, int> addr2lock_id;
};

LockConfig::LockConfig() : impl_(new LockConfigImpl()){
    assert(impl_);
}

LockConfig::~LockConfig() {}

comm::RetCode LockConfig::ReadConfig(proto::LockConfig &proto) {
    // sample
    proto.Clear();

    proto::Lock *lock = nullptr;
    comm::proto::Addr *addr = nullptr;

    // lock 1
    {
        lock = proto.add_locks();
        lock->set_lock_id(1);
        lock->set_scale(100);

        addr = lock->add_addrs();
        addr->set_ip("127.0.0.1");
        addr->set_port(7100);
        addr->set_paxos_port(7101);

        addr = lock->add_addrs();
        addr->set_ip("127.0.0.1");
        addr->set_port(7200);
        addr->set_paxos_port(7201);

        addr = lock->add_addrs();
        addr->set_ip("127.0.0.1");
        addr->set_port(7300);
        addr->set_paxos_port(7301);
    }

    return comm::RetCode::RET_OK;
}


comm::RetCode LockConfig::Rebuild() {
    bool need_check = NeedCheck();

    impl_->lock_id2lock.clear();
    impl_->addr2lock_id.clear();

    auto &&proto = GetProto();

    for (int i{0}; i < proto.locks_size(); ++i) {
        const auto &lock(proto.locks(i));
        if (!lock.lock_id()) continue;
        if (need_check) PHX_ASSERT(impl_->lock_id2lock.end() == impl_->lock_id2lock.find(lock.lock_id()), ==, true);
        impl_->lock_id2lock.emplace(lock.lock_id(), make_shared<proto::Lock>(lock));

        for (int j{0}; j < lock.addrs_size(); ++j) {
            auto &&addr = lock.addrs(j);
            if (need_check) PHX_ASSERT(impl_->addr2lock_id.end() == impl_->addr2lock_id.find(comm::utils::EncodeAddr(addr)), ==, true);
            impl_->addr2lock_id.emplace(comm::utils::EncodeAddr(addr), lock.lock_id());
        }
    }
    return comm::RetCode::RET_OK;
}


comm::RetCode LockConfig::GetAllLock(std::vector<shared_ptr<const proto::Lock> > &locks) const {
    for (auto &&it : impl_->lock_id2lock) {
        locks.push_back(it.second);
    }
    return comm::RetCode::RET_OK;
}

comm::RetCode LockConfig::GetAllLockID(std::set<int> &lock_ids) const {
    for (auto &&it : impl_->lock_id2lock) {
        lock_ids.insert(it.first);
    }
    return comm::RetCode::RET_OK;
}

comm::RetCode LockConfig::GetLockByLockID(const int lock_id, shared_ptr<const proto::Lock> &lock) const {
    auto it(impl_->lock_id2lock.find(lock_id));
    if (it == impl_->lock_id2lock.end()) return comm::RetCode::RET_ERR_RANGE_LOCK;
    lock = it->second;
    return comm::RetCode::RET_OK;
}

comm::RetCode LockConfig::GetLockIDByAddr(const comm::proto::Addr &addr, int &lock_id) const {
    auto &&encoded_addr = comm::utils::EncodeAddr(addr);
    auto &&it = impl_->addr2lock_id.find(encoded_addr);
    if (impl_->addr2lock_id.end() == it) return comm::RetCode::RET_ERR_RANGE_ADDR;
    lock_id = it->second;
    return comm::RetCode::RET_OK;
}

comm::RetCode LockConfig::GetLockByAddr(const comm::proto::Addr &addr, std::shared_ptr<const proto::Lock> &lock) const {
    comm::RetCode ret;

    int lock_id;
    if (comm::RetCode::RET_OK != (ret = GetLockIDByAddr(addr, lock_id))) return ret;
    return GetLockByLockID(lock_id, lock);
}


}  // namespace config

}  // namespace phxqueue


//gzrd_Lib_CPP_Version_ID--start
#ifndef GZRD_SVN_ATTR
#define GZRD_SVN_ATTR "0"
#endif
static char gzrd_Lib_CPP_Version_ID[] __attribute__((used))="$HeadURL$ $Id$ " GZRD_SVN_ATTR "__file__";
// gzrd_Lib_CPP_Version_ID--end

