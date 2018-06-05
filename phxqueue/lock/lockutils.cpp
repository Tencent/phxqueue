/*
Tencent is pleased to support the open source community by making PhxQueue available.
Copyright (C) 2017 THL A29 Limited, a Tencent company. All rights reserved.
Licensed under the BSD 3-Clause License (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at

<https://opensource.org/licenses/BSD-3-Clause>

Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.
*/



#include "phxqueue/lock/lockutils.h"

#include "phxqueue/comm.h"
#include "phxqueue/config.h"



namespace phxqueue {

namespace lock {


using namespace std;


// TODO:
//comm::RetCode GetAllIpPort(const int topic_id, const int group_id, vector<IpPort> &ip_ports) {
//    shared_ptr<const GroupConfig> group_conf;
//    comm::RetCode ret{GlobalConfig::GetThreadInstance()->GetGroupConfig(topic_id, group_config)};
//    if (comm::RetCode::RET_OK != ret) {
//        QLErr("GetGroupConfig err %d tid %d", ret, topic_id);
//
//        return comm::RetCode::RET_ERR_LOGIC;
//    }
//
//    shared_ptr<const phxqueue_proto::Group> group;
//    ret = group_conf->GetGroupByGroupID(group_id, group);
//    if (comm::RetCode::RET_OK != ret) {
//        QLErr("GetGroupByGroupID err %d tid %d gid %d", ret, topic_id, group_id);
//
//        return comm::RetCode::RET_ERR_LOGIC;
//    }
//    if (!group) {
//        QLErr("GetGroupByGroupID err group null tid %d gid %d", topic_id, group_id);
//
//        return comm::RetCode::RET_ERR_RANGE_GROUP;
//    }
//
//    return GetAllIpPort(group, ip_ports);
//}
//
//comm::RetCode GetAllIpPort(shared_ptr<const phxqueue_proto::Group> group, vector<IpPort> &ip_ports) {
//    if (!group) {
//        QLErr("group null");
//
//        return comm::RetCode::RET_ERR_RANGE_GROUP;
//    }
//
//    ip_ports.clear();
//
//    for (auto &&addr : group->addrs()) {
//        vector<string> addrs;
//        LOG_STATUS("group %d addr \"%s\"", group->group_id(), addr.c_str());
//
//        IpPort ip_port;
//        int paxos_port{-1};
//        bool succ{ParseAddr(addr, &(ip_port.ip), &(ip_port.port), &paxos_port)};
//        if (!succ) {
//            QLErr("ParseAddr err gid %d addr \"%s\"", group->group_id(), addr.c_str());
//
//            return comm::RetCode::RET_ERR_LOGIC;
//        }
//        LOG_STATUS("svr %s:%d", ip_port.ip.c_str(), ip_port.port);
//
//        ip_ports.emplace_back(move(ip_port));
//    }
//
//    return comm::RetCode::RET_OK;
//}

void LocalRecordInfo2LockInfo(const proto::LocalRecordInfo &local_record_info,
                              comm::proto::LockInfo &lock_info) {
    lock_info.set_version(local_record_info.version());
    lock_info.set_client_id(local_record_info.value());
    lock_info.set_lease_time_ms(local_record_info.lease_time_ms());
}

void LockInfo2LocalRecordInfo(const comm::proto::LockInfo &lock_info,
                              proto::LocalRecordInfo &local_record_info,
                              const uint64_t expire_time_ms) {
    local_record_info.set_version(lock_info.version());
    local_record_info.set_value(lock_info.client_id());
    local_record_info.set_lease_time_ms(lock_info.lease_time_ms());
    local_record_info.set_expire_time_ms(expire_time_ms);
}

//void GetOssAttrId(int32_t paxos_port, uint32_t *const oss_attr_id) {
//    const string local_host{LOGID_BASE::GetInnerIP()};
//    int topic_id{-1};
//    int ret{MMPHXLockGlobalConfig::GetDefault()->
//            GetTopicIDAndGroupIDByPaxosAddr(local_host, paxos_port, &topic_id, nullptr)};
//    if (comm::RetCode::RET_OK != ret) {
//        QLErr("GetTopicIDAndGroupIDByPaxosAddr err %d", ret);
//    } else {
//        const MMPHXLockTopicConfig *topic_conf{nullptr};
//        ret = MMPHXLockGlobalConfig::GetDefault()->GetTopicConfigByTopicID(topic_id, topic_conf);
//        if (oss_attr_id && 0 < topic_conf->GetConfig()->topic().oss_attr_id())
//            *oss_attr_id = topic_conf->GetConfig()->topic().oss_attr_id();
//    }
//}


}  // namespace lock

}  // namespace phxqueue

