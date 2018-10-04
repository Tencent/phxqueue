#pragma once
#include "phxqueue/comm.h"
#include "phxqueue/consumer.h"

#include "phxqueue_phxrpc/txstatus.h"
#include "phxqueue_phxrpc/consumer/subscribercaller.h"


namespace phxqueue_phxrpc {

namespace consumer {

class TxQueryHandler :
    public phxqueue::consumer::TxQueryHandler,
    virtual public phxqueue_phxrpc::txstatus::TxStatusReader,
    public phxqueue_phxrpc::consumer::SubscriberCaller<phxqueue::comm::proto::TxQueryRequest, phxqueue::comm::proto::TxQueryResponse> {
public:
    TxQueryHandler() : phxqueue::consumer::TxQueryHandler(), phxqueue_phxrpc::consumer::SubscriberCaller<phxqueue::comm::proto::TxQueryRequest, phxqueue::comm::proto::TxQueryResponse>() {}
    virtual ~TxQueryHandler() {}
};

}
}
