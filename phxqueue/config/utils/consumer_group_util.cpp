#include <string>

#include "phxqueue/comm.h"

#include "phxqueue/config/globalconfig.h"
#include "phxqueue/config/storeconfig.h"
#include "phxqueue/config/topicconfig.h"


namespace phxqueue {

namespace config {

namespace utils {


using namespace std;


comm::RetCode GetConsumerGroupIDsByConsumerAddr(const int topic_id, const comm::proto::Addr &addr, std::set<int> &consumer_group_ids) {
    consumer_group_ids.clear();

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

    if (consumer->consumer_group_ids_size()) {
        for (int i{0}; i < consumer->consumer_group_ids_size(); ++i) {
            auto consumer_group_id = consumer->consumer_group_ids(i);
            if (topic_config->IsValidConsumerGroupID(consumer_group_id)) {
                consumer_group_ids.insert(consumer_group_id);
            }
        }
        return comm::RetCode::RET_OK;
    }

    if (comm::RetCode::RET_OK != (ret = topic_config->GetAllConsumerGroupID(consumer_group_ids))) {
        NLErr("GetAllConsumerGroupID ret %d", comm::as_integer(ret));
        return ret;
    }

    return comm::RetCode::RET_OK;
}


}  // namespace utils

}  // namespace config

}  // namespace phxqueue


