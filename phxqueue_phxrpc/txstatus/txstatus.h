#pragma once

#include "phxqueue/comm.h"
#include "phxqueue/txstatus.h"

namespace phxqueue_phxrpc {

namespace txstatus {

class TxStatusReader : virtual public phxqueue::txstatus::TxStatusReader
{
public:
    TxStatusReader() : phxqueue::txstatus::TxStatusReader() {}
    virtual ~TxStatusReader() override {}

protected:
    virtual phxqueue::comm::RetCode GetStatusInfoFromLock(const phxqueue::comm::proto::GetStringRequest &req, phxqueue::comm::proto::GetStringResponse &resp) override;
};

class TxStatusWriter : virtual public phxqueue::txstatus::TxStatusWriter, public phxqueue_phxrpc::txstatus::TxStatusReader
{
public:
    TxStatusWriter() : phxqueue::txstatus::TxStatusWriter(), phxqueue_phxrpc::txstatus::TxStatusReader() {}
    virtual ~TxStatusWriter() override {}

protected:
    virtual phxqueue::comm::RetCode SetStatusInfoToLock(const phxqueue::comm::proto::SetStringRequest &req, phxqueue::comm::proto::SetStringResponse &resp) override;
};

}
}
