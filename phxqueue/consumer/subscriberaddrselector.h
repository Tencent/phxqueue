#pragma once
#include "phxqueue/comm.h"
#include "phxqueue/config.h"

namespace phxqueue {

namespace consumer {

class SubscriberAddrSelector {
public:
    SubscriberAddrSelector() {}
    virtual ~SubscriberAddrSelector() {}
    virtual comm::RetCode SelectSubscriberAddr(const comm::proto::QItem &item, const int sub_id, comm::proto::Addr &addr, config::proto::RouteGeneral &route_general);
};

}
}
