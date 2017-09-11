#pragma once

#include <string>
#include <vector>


namespace phxqueue {

namespace config {

namespace utils {


comm::RetCode GetPubIDsByStoreID(const int topic_id, const int store_id, std::set<int> &pub_ids);


}  // namespace utils

}  // namespace config

}  // namespace phxqueue

