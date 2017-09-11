/*
Tencent is pleased to support the open source community by making PhxQueue available.
Copyright (C) 2017 THL A29 Limited, a Tencent company. All rights reserved.
Licensed under the BSD 3-Clause License (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at

<https://opensource.org/licenses/BSD-3-Clause>

Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.
*/



#include "phxqueue/comm/masterclient.h"

#include "phxqueue/comm/utils.h"


namespace phxqueue {

namespace comm {


using namespace std;


thread_local map<std::string, std::pair<proto::Addr, uint64_t>> MasterClientBase::addr_cache_;

MasterClientBase::MasterClientBase() {}

MasterClientBase::~MasterClientBase() {}


void MasterClientBase::PutAddrToCache(const std::string &key, const proto::Addr &addr) {
    addr_cache_[key] = make_pair(addr, time(nullptr) + 3600 + utils::OtherUtils::FastRand() % 1800);
}

bool MasterClientBase::GetAddrFromCache(const std::string &key, proto::Addr &addr) {
    auto &&it(addr_cache_.find(key));
    if (it != addr_cache_.end()) {
        if (static_cast<uint64_t>(time(nullptr)) > it->second.second) return false;
        addr = it->second.first;
        return true;
    }
    return false;
}

void MasterClientBase::RemoveCache(const std::string &key) {
    addr_cache_.erase(key);
}


}  // namespace comm

}  // namespace phxqueue

