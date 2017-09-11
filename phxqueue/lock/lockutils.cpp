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

void LocalLockInfo2LockInfo(const proto::LocalLockInfo &local_lock_info,
                            comm::proto::LockInfo &lock_info) {
    lock_info.set_version(local_lock_info.version());
    lock_info.set_client_id(local_lock_info.client_id());
    lock_info.set_lease_time_ms(local_lock_info.lease_time_ms());
}

void LockInfo2LocalLockInfo(const comm::proto::LockInfo &lock_info,
                            proto::LocalLockInfo &local_lock_info,
                            const uint64_t expire_time_ms) {
    local_lock_info.set_version(lock_info.version());
    local_lock_info.set_client_id(lock_info.client_id());
    local_lock_info.set_lease_time_ms(lock_info.lease_time_ms());
    local_lock_info.set_expire_time_ms(expire_time_ms);
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


//gzrd_Lib_CPP_Version_ID--start
#ifndef GZRD_SVN_ATTR
#define GZRD_SVN_ATTR "0"
#endif
static char gzrd_Lib_CPP_Version_ID[] __attribute__((used))="$HeadURL: http://scm-gy.tencent.com/gzrd/gzrd_mail_rep/phoenix_proj/trunk/phxqueue/src/lock/lockutils.cpp $ $Id: lockutils.cpp 2102034 2017-05-25 13:23:49Z walnuthe $ " GZRD_SVN_ATTR "__file__";
// gzrd_Lib_CPP_Version_ID--end

