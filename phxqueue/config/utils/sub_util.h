#pragma once


#include <string>
#include <vector>


namespace phxqueue {

namespace config {

namespace utils {


comm::RetCode GetSubIDsByConsumerAddr(const int topic_id, const comm::proto::Addr &addr, std::set<int> &sub_ids);


}  // namespace utils

}  // namespace config

}  // namespace phxqueue

