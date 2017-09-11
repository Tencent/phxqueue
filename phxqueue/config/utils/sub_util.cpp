#include <string>

#include "phxqueue/comm.h"

#include "phxqueue/config/globalconfig.h"
#include "phxqueue/config/storeconfig.h"
#include "phxqueue/config/topicconfig.h"


namespace phxqueue {

namespace config {

namespace utils {


using namespace std;


comm::RetCode GetSubIDsByConsumerAddr(const int topic_id, const comm::proto::Addr &addr, std::set<int> &sub_ids) {
    sub_ids.clear();

    comm::RetCode ret;

    shared_ptr<const config::ConsumerConfig> consumer_config;
    if (comm::RetCode::RET_OK != (ret = config::GlobalConfig::GetThreadInstance()->GetConsumerConfig(topic_id, consumer_config))) {
        NLErr("GetConsumerConfig ret %d", comm::as_integer(ret));
        return ret;
    }

    shared_ptr<const config::proto::Consumer> consumer;
    if (comm::RetCode::RET_OK != (ret = consumer_config->GetConsumerByAddr(addr, consumer))) {
        NLErr("GetConsumerByAddr ret %d addr(%s:%d)", comm::as_integer(ret), addr.ip().c_str(), addr.port());
        return ret;
    }

    shared_ptr<const config::TopicConfig> topic_config;
    if (comm::RetCode::RET_OK != (ret = config::GlobalConfig::GetThreadInstance()->GetTopicConfigByTopicID(topic_id, topic_config))) {
        NLErr("GetTopicConfigByTopicID ret %d", comm::as_integer(ret));
        return ret;
    }

    set<int> ignore_sub_ids;
    for (int i{0}; i < topic_config->GetProto().topic().consumer_ignore_sub_ids_size(); ++i) {
        ignore_sub_ids.insert(topic_config->GetProto().topic().consumer_ignore_sub_ids(i));
    }

    if (consumer->sub_ids_size()) {
        for (int i{0}; i < consumer->sub_ids_size(); ++i) {
            auto sub_id = consumer->sub_ids(i);
            if (topic_config->IsValidSubID(sub_id) && ignore_sub_ids.end() == ignore_sub_ids.find(sub_id)) {
                sub_ids.insert(sub_id);
            }
        }
        return comm::RetCode::RET_OK;
    }

    if (comm::RetCode::RET_OK != (ret = topic_config->GetAllSubID(sub_ids))) {
        NLErr("GetAllSubID ret %d", comm::as_integer(ret));
        return ret;
    }

    for (auto &&ignore_sub_id : ignore_sub_ids) {
        sub_ids.erase(ignore_sub_id);
    }

    return comm::RetCode::RET_OK;
}


}  // namespace utils

}  // namespace config

}  // namespace phxqueue


//gzrd_Lib_CPP_Version_ID--start
#ifndef GZRD_SVN_ATTR
#define GZRD_SVN_ATTR "0"
#endif
static char gzrd_Lib_CPP_Version_ID[] __attribute__((used))="$HeadURL$ $Id$ " GZRD_SVN_ATTR "__file__";
// gzrd_Lib_CPP_Version_ID--end

