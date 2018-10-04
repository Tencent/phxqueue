#pragma once
#include "phxqueue/comm.h"

namespace phxqueue {

namespace consumer {

class SubscriberAddrSelector {
public:
    SubscriberAddrSelector() {}
    virtual ~SubscriberAddrSelector() {}
    virtual comm::RetCode SelectSubscriberAddr(comm::proto::Cookies &sys_cookies, const int topic_id, const int sub_id, const uint64_t uin, comm::proto::Addr &addr);
};

}
}
