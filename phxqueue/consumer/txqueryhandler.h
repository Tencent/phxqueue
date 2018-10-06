#pragma once
#include "phxqueue/comm.h"
#include "phxqueue/txstatus.h"

#include "phxqueue/consumer/subscriberaddrselector.h"
#include "phxqueue/consumer/subscribercaller.h"

namespace phxqueue {

namespace consumer {


class TxQueryHandler :
    public comm::Handler,
    virtual public txstatus::TxStatusReader,
    virtual public SubscriberAddrSelector,
    virtual public SubscriberCaller<comm::proto::TxQueryRequest, comm::proto::TxQueryResponse> {
public:
    TxQueryHandler() : comm::Handler(), txstatus::TxStatusReader(), SubscriberAddrSelector(), SubscriberCaller<comm::proto::TxQueryRequest, comm::proto::TxQueryResponse>() {}
    virtual ~TxQueryHandler() {}

    virtual comm::HandleResult Handle(const comm::proto::ConsumerContext &cc, comm::proto::QItem &item, std::string &uncompressed_buffer);

protected:
    comm::RetCode GetTxStatus(const int topic_id, const int pub_id, const std::string& client_id, comm::proto::StatusInfo &status_info);
};


}
}
