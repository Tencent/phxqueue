#pragma once
#include "phxqueue/comm.h"
#include "phxqueue/config.h"

namespace phxqueue {

namespace consumer {

template <typename Req, typename Resp>
class SubscriberCaller {
public:
    SubscriberCaller() {}
    virtual ~SubscriberCaller() {}
    virtual comm::RetCode CallSubscriber(const comm::proto::QItem &item, const int sub_id, const Req &req, Resp &resp, config::proto::RouteGeneral &route_general) = 0;
};

}
}
