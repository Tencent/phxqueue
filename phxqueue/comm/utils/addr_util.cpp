/*
Tencent is pleased to support the open source community by making PhxQueue available.
Copyright (C) 2017 THL A29 Limited, a Tencent company. All rights reserved.
Licensed under the BSD 3-Clause License (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at

<https://opensource.org/licenses/BSD-3-Clause>

Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.
*/



#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>

#include "phxqueue/comm/utils/addr_util.h"
#include "phxqueue/comm/utils/string_util.h"


namespace phxqueue {

namespace comm {

namespace utils {


using namespace std;


uint64_t EncodeAddr(const proto::Addr &addr) {
    uint64_t res = inet_addr(addr.ip().c_str());
    res = (res << 16) | addr.port();
    res = (res << 16) | addr.paxos_port();
    return res;

}

void DecodeAddr(const uint64_t encoded_addr, proto::Addr &addr) {
    in_addr in;
    in.s_addr = (encoded_addr >> 32);

    addr.set_ip(inet_ntoa(in));
    addr.set_port((encoded_addr >> 16) & 0xFFFF);
    addr.set_paxos_port(encoded_addr & 0xFFFF);
}

string EncodedAddrToIPString(const uint64_t encoded_addr) {
    proto::Addr addr;
    DecodeAddr(encoded_addr, addr);

    return addr.ip();
}

string AddrToString(const proto::Addr &addr) {
    return addr.ip() + ":" + to_string(addr.port()) + ":" + to_string(addr.paxos_port());
}

string AddrScaleToString(const proto::AddrScale &addr_scale) {
    return AddrToString(addr_scale.addr()) + "@" + to_string(addr_scale.scale());
}

string AddrScalesToString(const google::protobuf::RepeatedPtrField
                          <proto::AddrScale> &addr_scales) {
    string s;
    for (const auto &addr_scale : addr_scales) {
        s += AddrScaleToString(addr_scale) + ";";
    }

    return s;
}


}  // namespace utils

}  // namespace comm

}  // namespace phxqueue

