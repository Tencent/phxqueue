#include "phxrpc_scheduler.h"

#include "lock_client.h"


namespace phxqueue_phxrpc {

namespace scheduler {


PhxRpcScheduler::PhxRpcScheduler(const phxqueue::SchedulerOption &opt)
        : phxqueue::Scheduler(opt) {}

PhxRpcScheduler::~PhxRpcScheduler() {}

phxqueue::RetCode
PhxRpcScheduler::GetLockInfo(const phxqueue_proto::GetLockInfoRequest &req,
                             phxqueue_proto::GetLockInfoResponse &resp) {
    LockClient client;
    return client.ProtoGetLockInfo(req, resp);
}

phxqueue::RetCode
PhxRpcScheduler::AcquireLock(const phxqueue_proto::AcquireLockRequest &req,
                             phxqueue_proto::AcquireLockResponse &resp) {
    LockClient client;
    return client.ProtoAcquireLock(req, resp);
}


}

}


//gzrd_Lib_CPP_Version_ID--start
#ifndef GZRD_SVN_ATTR
#define GZRD_SVN_ATTR "0"
#endif
static char gzrd_Lib_CPP_Version_ID[] __attribute__((used))="$HeadURL: http://scm-gy.tencent.com/gzrd/gzrd_mail_rep/phoenix_proj/trunk/phxqueue/phxqueue_phxrpc/src/scheduler/phxrpc_scheduler.cpp $ $Id: phxrpc_scheduler.cpp 2142737 2017-06-28 14:20:06Z walnuthe $ " GZRD_SVN_ATTR "__file__";
// gzrd_Lib_CPP_Version_ID--end

