#include "subscriberaddrselector.h"

#include "phxqueue/config.h"

namespace phxqueue {

namespace consumer {

using namespace std;

comm::RetCode SubscriberAddrSelector:: SelectSubscriberAddr(comm::proto::Cookies &sys_cookies, const int topic_id, const int sub_id, const uint64_t uin, comm::proto::Addr &addr) {
    addr.Clear();

    comm::RetCode ret;
    shared_ptr<const config::TopicConfig> topic_config;
    if (comm::RetCode::RET_OK != (ret = config::GlobalConfig::GetThreadInstance()->GetTopicConfigByTopicID(topic_id, topic_config))) {
        QLErr("GetTopicConfigByTopicID ret %d topic_id %d", comm::as_integer(ret), topic_id);
        return ret;
    }

    shared_ptr<const config::RouteConfig> route_config;
    if (comm::RetCode::RET_OK != (ret = topic_config->GetRouteConfigBySubID(sub_id, route_config))) {
        QLErr("GetRouteConfigBySubID ret %d topic_id %d sub_id %d", comm::as_integer(ret), topic_id, sub_id);
        return ret;
    }

    if (comm::RetCode::RET_OK != (ret = route_config->GetAddrByConsistentHash(uin, addr))) {
        QLErr("GetAddrByConsistentHash ret %d topic_id %d sub_id %d", comm::as_integer(ret), topic_id, sub_id);
        return ret;
    }

    return comm::RetCode::RET_OK;
}

}
}
