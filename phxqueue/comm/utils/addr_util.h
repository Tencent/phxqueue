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

