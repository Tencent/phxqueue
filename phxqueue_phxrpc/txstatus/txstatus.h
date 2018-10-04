#pragma once

#include "phxqueue/comm.h"
#include "phxqueue/txstatus.h"

namespace phxqueue_phxrpc {

namespace txstatus {

using namespace std;

class TxStatusReader : virtual public phxqueue::txstatus::TxStatusReader
{
public:
    TxStatusReader() : phxqueue::txstatus::TxStatusReader() {}
    virtual ~TxStatusReader() {}

protected:
    virtual phxqueue::comm::RetCode GetStatusInfoFromLock(const phxqueue::comm::proto::GetStringRequest &req, phxqueue::comm::proto::GetStringResponse &resp);
};

class TxStatusWriter : public phxqueue_phxrpc::txstatus::TxStatusReader, virtual public phxqueue::txstatus::TxStatusWriter
{
public:
    TxStatusWriter() : phxqueue_phxrpc::txstatus::TxStatusReader(), phxqueue::txstatus::TxStatusWriter() {}
    virtual ~TxStatusWriter() {}

protected:
    virtual phxqueue::comm::RetCode SetStatusInfoToLock(const phxqueue::comm::proto::SetStringRequest &req, phxqueue::comm::proto::SetStringResponse &resp);
};

}
}
