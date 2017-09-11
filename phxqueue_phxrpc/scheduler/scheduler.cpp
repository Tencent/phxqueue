#include "phxqueue_phxrpc/scheduler/scheduler.h"

#include <cinttypes>

#include "phxqueue/comm.h"

#include "phxqueue_phxrpc/app/lock/lock_client.h"


namespace phxqueue_phxrpc {

namespace scheduler {


Scheduler::Scheduler(const phxqueue::scheduler::SchedulerOption &opt)
        : phxqueue::scheduler::Scheduler(opt) {}

Scheduler::~Scheduler() {}

phxqueue::comm::RetCode
Scheduler::GetLockInfo(const phxqueue::comm::proto::GetLockInfoRequest &req,
                       phxqueue::comm::proto::GetLockInfoResponse &resp) {
    LockClient lock_client;
    auto ret(lock_client.ProtoGetLockInfo(req, resp));
    if (phxqueue::comm::RetCode::RET_OK != ret) {
        QLErr("ProtoGetLockInfo ret %d", phxqueue::comm::as_integer(ret));
    }
    return ret;
}

phxqueue::comm::RetCode
Scheduler::AcquireLock(const phxqueue::comm::proto::AcquireLockRequest &req,
                       phxqueue::comm::proto::AcquireLockResponse &resp) {
    LockClient lock_client;
    auto ret(lock_client.ProtoAcquireLock(req, resp));
    if (phxqueue::comm::RetCode::RET_OK != ret) {
        QLErr("ProtoAcquireLock ret %d", phxqueue::comm::as_integer(ret));
    }
    return ret;
}


}  // namespace scheduler

}  // namespace phxqueue_phxrpc


//gzrd_Lib_CPP_Version_ID--start
#ifndef GZRD_SVN_ATTR
#define GZRD_SVN_ATTR "0"
#endif
static char gzrd_Lib_CPP_Version_ID[] __attribute__((used))="$HeadURL: http://scm-gy.tencent.com/gzrd/gzrd_mail_rep/phoenix_proj/trunk/skphxqueue/src/scheduler/skscheduler.cpp $ $Id: skscheduler.cpp 2069994 2017-05-03 09:37:19Z walnuthe $ " GZRD_SVN_ATTR "__file__";
// gzrd_Lib_CPP_Version_ID--end

