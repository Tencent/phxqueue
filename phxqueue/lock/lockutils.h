/*
Tencent is pleased to support the open source community by making PhxQueue available.
Copyright (C) 2017 THL A29 Limited, a Tencent company. All rights reserved.
Licensed under the BSD 3-Clause License (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at

<https://opensource.org/licenses/BSD-3-Clause>

Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.
*/



#pragma once

#include <cinttypes>
#include <cstdarg>
#include <memory>
#include <string>
#include <typeinfo>

#include "phxqueue/comm.h"

#include "phxqueue/lock/proto/lock.pb.h"


namespace phxqueue {

namespace lock {


struct IpPort {
    std::string ip;
    int port{-1};
    IpPort() = default;
    IpPort(const std::string &ip2, int port2) : ip(ip2), port(port2) {}
};

// TODO:
//comm::RetCode GetAllIpPort(const int topic_id, const int group_id,
//                           std::vector<phxqueue::IpPort> &ip_ports);
//comm::RetCode GetAllIpPort(std::shared_ptr<const phxqueue_proto::Group> group,
//                           std::vector<phxqueue::IpPort> &ip_ports);

void LocalRecordInfo2LockInfo(const proto::LocalRecordInfo &local_record_info,
                              comm::proto::LockInfo &lock_info);
void LockInfo2LocalRecordInfo(const comm::proto::LockInfo &lock_info,
                              proto::LocalRecordInfo &local_record_info,
                              const uint64_t expire_time_ms);

//void GetOssAttrId(int32_t paxos_port, uint32_t *const oss_attr_id);


}  // namespace lock

}  // namespace phxqueue

