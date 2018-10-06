#pragma once
#include "phxqueue/comm.h"

#include "phxqueue/consumer/subscriberaddrselector.h"
#include "phxqueue/consumer/subscribercaller.h"

namespace phxqueue {

namespace consumer {


class PushHandler :
    public comm::Handler,
    virtual public SubscriberAddrSelector,
    virtual public SubscriberCaller<comm::proto::PushRequest, comm::proto::PushResponse> {
public:
    PushHandler() : comm::Handler(), SubscriberAddrSelector(), SubscriberCaller<comm::proto::PushRequest, comm::proto::PushResponse>() {}
    virtual ~PushHandler() {}

    virtual comm::HandleResult Handle(const comm::proto::ConsumerContext &cc, comm::proto::QItem &item, std::string &uncompressed_buffer);

};

}
}
