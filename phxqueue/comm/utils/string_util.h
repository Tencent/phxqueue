#pragma once

#include <string>
#include <vector>


namespace phxqueue {

namespace comm {

namespace utils {


void StrSplitList(const std::string &str, const std::string &delimiters, std::vector<std::string> &results);


}  // namespace utils

}  // namespace comm

}  // namespace phxqueue

