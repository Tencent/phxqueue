#include "phxqueue_phxrpc/consumer/consumer.h"

#include <sys/prctl.h>

#include <zlib.h>

#include "phxqueue/comm.h"
#include "phxqueue/config.h"
#include "phxqueue/consumer.h"

#include "phxqueue_phxrpc/app/lock/lock_client.h"
#include "phxqueue_phxrpc/app/scheduler/scheduler_client.h"
#include "phxqueue_phxrpc/app/store/store_client.h"


namespace phxqueue_phxrpc {

namespace consumer {


using namespace std;

Consumer::Consumer(const phxqueue::consumer::ConsumerOption &opt) : phxqueue::consumer::Consumer(opt) {}

Consumer::~Consumer() {}

phxqueue::comm::RetCode
Consumer::UncompressBuffer(const string &buffer, const int buffer_type,
                           string &uncompressed_buffer) {
    uncompressed_buffer = buffer;
    return phxqueue::comm::RetCode::RET_OK;
}

void Consumer::CompressBuffer(const string &buffer, string &compressed_buffer,
                              const int buffer_type) {
    compressed_buffer = buffer;
}

phxqueue::comm::RetCode
Consumer::Get(const phxqueue::comm::proto::GetRequest &req,
              phxqueue::comm::proto::GetResponse &resp) {

    static __thread StoreClient store_client;
    auto ret = store_client.ProtoGet(req, resp);
    if (phxqueue::comm::RetCode::RET_OK != ret) {
        QLErr("ProtoGet ret %d", phxqueue::comm::as_integer(ret));
    }
    return ret;
}

phxqueue::comm::RetCode
Consumer::Add(const phxqueue::comm::proto::AddRequest &req,
              phxqueue::comm::proto::AddResponse &resp) {

    static __thread StoreClient store_client;
    auto ret = store_client.ProtoAdd(req, resp);
    if (phxqueue::comm::RetCode::RET_OK != ret) {
        QLErr("ProtoAdd ret %d", phxqueue::comm::as_integer(ret));
    }
    return ret;
}

phxqueue::comm::RetCode
Consumer::GetAddrScale(const phxqueue::comm::proto::GetAddrScaleRequest &req,
                       phxqueue::comm::proto::GetAddrScaleResponse &resp) {
    static __thread SchedulerClient scheduler_client;
    auto ret = scheduler_client.ProtoGetAddrScale(req, resp);
    if (phxqueue::comm::RetCode::RET_OK != ret) {
        QLErr("ProtoGetAddrScale ret %d", phxqueue::comm::as_integer(ret));
    }
    return ret;
}

phxqueue::comm::RetCode
Consumer::GetLockInfo(const phxqueue::comm::proto::GetLockInfoRequest &req,
                      phxqueue::comm::proto::GetLockInfoResponse &resp) {
    static __thread LockClient lock_client;
    auto ret = lock_client.ProtoGetLockInfo(req, resp);
    if (phxqueue::comm::RetCode::RET_OK != ret) {
        QLErr("ProtoGetLockInfo ret %d", phxqueue::comm::as_integer(ret));
    }
    return ret;
}

phxqueue::comm::RetCode
Consumer::AcquireLock(const phxqueue::comm::proto::AcquireLockRequest &req,
                      phxqueue::comm::proto::AcquireLockResponse &resp) {
    static __thread LockClient lock_client;
    auto ret = lock_client.ProtoAcquireLock(req, resp);
    if (phxqueue::comm::RetCode::RET_OK != ret) {
        QLErr("ProtoAcquireLock ret %d", phxqueue::comm::as_integer(ret));
    }
    return ret;
}


}  // namespace consumer

}  // namespace phxqueue_phxrpc


//gzrd_Lib_CPP_Version_ID--start
#ifndef GZRD_SVN_ATTR
#define GZRD_SVN_ATTR "0"
#endif
static char gzrd_Lib_CPP_Version_ID[] __attribute__((used))="$HeadURL$ $Id$ " GZRD_SVN_ATTR "__file__";
// gzrd_Lib_CPP_Version_ID--end

