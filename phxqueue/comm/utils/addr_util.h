/*
Tencent is pleased to support the open source community by making PhxQueue available.
Copyright (C) 2017 THL A29 Limited, a Tencent company. All rights reserved.
Licensed under the BSD 3-Clause License (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at

<https://opensource.org/licenses/BSD-3-Clause>

Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.
*/



#pragma once

#include <string>
#include <vector>


#include "phxqueue/comm/proto/comm.pb.h"


namespace phxqueue {

namespace comm {

namespace utils {


uint64_t EncodeAddr(const proto::Addr &addr);
void DecodeAddr(const uint64_t encoded_addr, proto::Addr &addr);
std::string EncodedAddrToIPString(const uint64_t encoded_addr);
std::string AddrToString(const proto::Addr &addr);
std::string AddrScaleToString(const proto::AddrScale &addr_scale);
std::string AddrScalesToString(const google::protobuf::RepeatedPtrField
                               <proto::AddrScale> &addr_scales);

}  // namespace utils

}  // namespace comm

}  // namespace phxqueue

