#pragma once


#include <string>
#include <vector>


namespace phxqueue {

namespace config {

namespace utils {


comm::RetCode GetConsumerGroupIDsByConsumerAddr(const int topic_id, const comm::proto::Addr &addr, std::set<int> &consumer_group_ids);


}  // namespace utils

}  // namespace config

}  // namespace phxqueue

