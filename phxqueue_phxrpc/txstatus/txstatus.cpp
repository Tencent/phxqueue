#include "phxqueue_phxrpc/txstatus/txstatus.h"

#include "phxqueue_phxrpc/app/lock/lock_client.h"

namespace phxqueue_phxrpc {

namespace txstatus {

using namespace std;

/***   TxStatusReader      ***/

phxqueue::comm::RetCode TxStatusReader::GetStatusInfoFromLock(const phxqueue::comm::proto::GetStringRequest &req, phxqueue::comm::proto::GetStringResponse &resp)
{
    LockClient lock_client;
    auto ret = lock_client.ProtoGetString(req, resp);
    if (phxqueue::comm::RetCode::RET_OK != ret) {
        QLErr("GetString ret %d", phxqueue::comm::as_integer(ret));
    }
    return ret;
}

/***   TxStatusWriter      ***/

phxqueue::comm::RetCode TxStatusWriter::SetStatusInfoToLock(const phxqueue::comm::proto::SetStringRequest &req, phxqueue::comm::proto::SetStringResponse &resp)
{
    LockClient lock_client;
    auto ret = lock_client.ProtoSetString(req, resp);
    if (phxqueue::comm::RetCode::RET_OK != ret) {
        QLErr("SetString ret %d", phxqueue::comm::as_integer(ret));
    }
    return ret;
}


}
}
