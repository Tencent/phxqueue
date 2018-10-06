#pragma once
#include "phxqueue/comm.h"
#include "phxqueue/consumer.h"

#include "phxqueue_phxrpc/consumer/subscribercaller.h"


namespace phxqueue_phxrpc {

namespace consumer {

class PushHandler :
    public phxqueue::consumer::PushHandler,
    virtual public phxqueue_phxrpc::consumer::SubscriberCaller<phxqueue::comm::proto::PushRequest, phxqueue::comm::proto::PushResponse> {
public:
    PushHandler() : phxqueue::consumer::PushHandler(), phxqueue_phxrpc::consumer::SubscriberCaller<phxqueue::comm::proto::PushRequest, phxqueue::comm::proto::PushResponse>() {}
    virtual ~PushHandler() {}
};


}
}
