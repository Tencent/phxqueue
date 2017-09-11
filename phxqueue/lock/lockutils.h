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
//                     std::vector<phxqueue::IpPort> &ip_ports);
//comm::RetCode GetAllIpPort(std::shared_ptr<const phxqueue_proto::Group> group,
//                     std::vector<phxqueue::IpPort> &ip_ports);

void LocalLockInfo2LockInfo(const proto::LocalLockInfo &local_lock_info,
                            comm::proto::LockInfo &lock_info);
void LockInfo2LocalLockInfo(const comm::proto::LockInfo &lock_info,
                            proto::LocalLockInfo &local_lock_info,
                            const uint64_t expire_time_ms);

//void GetOssAttrId(int32_t paxos_port, uint32_t *const oss_attr_id);


}  // namespace lock

}  // namespace phxqueue

