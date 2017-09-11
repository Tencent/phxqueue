#include <string>

#include "phxqueue/comm/utils/string_util.h"


namespace phxqueue {

namespace comm {

namespace utils {


using namespace std;


void StrSplitList(const string &str, const string &delimiters, vector<std::string> &results) {
    results.clear();
    auto last = 0;
    auto found = str.find_first_of(delimiters);
    while (string::npos != found) {
        auto r = str.substr(last, found - last);
        last = found + 1;
        found = str.find_first_of(delimiters, last);
        if (!r.empty()) results.push_back(r);
    }
    auto r = str.substr(last);
    if (!r.empty()) results.push_back(r);
}


}  // namespace utils

}  // namespace comm

}  // namespace phxqueue


//gzrd_Lib_CPP_Version_ID--start
#ifndef GZRD_SVN_ATTR
#define GZRD_SVN_ATTR "0"
#endif
static char gzrd_Lib_CPP_Version_ID[] __attribute__((used))="$HeadURL$ $Id$ " GZRD_SVN_ATTR "__file__";
// gzrd_Lib_CPP_Version_ID--end

