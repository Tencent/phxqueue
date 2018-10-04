#pragma once
#include "phxqueue/comm.h"

namespace phxqueue {

namespace consumer {

template <typename Req, typename Resp>
class SubscriberCaller {
public:
    SubscriberCaller() {}
    virtual ~SubscriberCaller() {}
    virtual comm::RetCode CallSubscriber(const Req &req, Resp &resp) = 0;
};

}
}
