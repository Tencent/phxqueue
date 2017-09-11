#include "phxqueue/test/simplescheduler.h"

#include <iostream>
#include <memory>

#include "phxqueue/comm.h"
#include "phxqueue/config.h"


namespace phxqueue {

namespace test {


using namespace std;


comm::RetCode SimpleScheduler::GetLockInfo(const comm::proto::GetLockInfoRequest &req,
                                           comm::proto::GetLockInfoResponse &resp) {
    QLVerb("GetLockInfo topic_id %d lock_id %d lock_key %s",
           req.topic_id(), req.lock_id(), req.lock_key().c_str());

    shared_ptr<const config::LockConfig> lock_config;
    comm::RetCode ret{config::GlobalConfig::GetThreadInstance()->
            GetLockConfig(req.topic_id(), lock_config)};
    if (comm::RetCode::RET_OK != ret) {
        QLErr("GetLockConfig ret %d topic_id %d", as_integer(ret), req.topic_id());

        return ret;
    }

    shared_ptr<const config::proto::Lock> lock;
    ret = lock_config->GetLockByLockID(req.lock_id(), lock);
    if (comm::RetCode::RET_OK != ret) {
        QLErr("GetLockByLockID ret %d lock_id %d", as_integer(ret), req.lock_id());

        return ret;
    }

    assert(lock->addrs_size() >= 2);
    comm::proto::Addr master_addr = lock->addrs(1);

    if (req.master_addr().ip() == master_addr.ip() &&
        req.master_addr().port() == master_addr.port() &&
        req.master_addr().paxos_port() == master_addr.paxos_port()) {

        auto &&lock_info(resp.mutable_lock_info());
        lock_info->set_lock_key(req.lock_key());
        lock_info->set_version(1);
        lock_info->set_client_id("test_client_1");
        lock_info->set_lease_time_ms(10000000);

        return comm::RetCode::RET_OK;
    }
    resp.mutable_redirect_addr()->CopyFrom(master_addr);

    return comm::RetCode::RET_ERR_NOT_MASTER;
}

comm::RetCode SimpleScheduler::AcquireLock(const comm::proto::AcquireLockRequest &req,
                                           comm::proto::AcquireLockResponse &resp) {
    QLVerb("AcquireLock topic_id %d lock_id %d lock_key %s",
           req.topic_id(), req.lock_id(), req.lock_info().lock_key().c_str());

    shared_ptr<const config::LockConfig> lock_config;
    comm::RetCode ret{config::GlobalConfig::GetThreadInstance()->
            GetLockConfig(req.topic_id(), lock_config)};
    if (comm::RetCode::RET_OK != ret) {
        QLErr("GetLockConfig ret %d topic_id %d", as_integer(ret), req.topic_id());

        return ret;
    }

    shared_ptr<const config::proto::Lock> lock;
    ret = lock_config->GetLockByLockID(req.lock_id(), lock);
    if (comm::RetCode::RET_OK != ret) {
        QLErr("GetLockByLockID ret %d lock_id %d", as_integer(ret), req.lock_id());

        return ret;
    }

    assert(lock->addrs_size() >= 2);
    comm::proto::Addr master_addr = lock->addrs(1);

    if (req.master_addr().ip() == master_addr.ip() &&
        req.master_addr().port() == master_addr.port() &&
        req.master_addr().paxos_port() == master_addr.paxos_port()) {

        return comm::RetCode::RET_OK;
    }
    resp.mutable_redirect_addr()->CopyFrom(master_addr);

    return comm::RetCode::RET_ERR_NOT_MASTER;
}


}  // namespace test

}  // namespace phxqueue


//gzrd_Lib_CPP_Version_ID--start
#ifndef GZRD_SVN_ATTR
#define GZRD_SVN_ATTR "0"
#endif
static char gzrd_Lib_CPP_Version_ID[] __attribute__((used))="$HeadURL: http://scm-gy.tencent.com/gzrd/gzrd_mail_rep/phoenix_proj/trunk/phxqueue/src/test/simplescheduler.cpp $ $Id: simplescheduler.cpp 2124396 2017-06-15 03:58:06Z walnuthe $ " GZRD_SVN_ATTR "__file__";
// gzrd_Lib_CPP_Version_ID--end

