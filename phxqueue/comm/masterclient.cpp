#include "phxqueue/comm/masterclient.h"

#include "phxqueue/comm/utils.h"


namespace phxqueue {

namespace comm {


using namespace std;


thread_local map<std::string, std::pair<proto::Addr, uint64_t>> MasterClientBase::addr_cache_;

MasterClientBase::MasterClientBase() {}

MasterClientBase::~MasterClientBase() {}


void MasterClientBase::PutAddrToCache(const std::string &key, const proto::Addr &addr) {
    addr_cache_[key] = make_pair(addr, time(nullptr) + 3600 + utils::OtherUtils::FastRand() % 1800);
}

bool MasterClientBase::GetAddrFromCache(const std::string &key, proto::Addr &addr) {
    auto &&it(addr_cache_.find(key));
    if (it != addr_cache_.end()) {
        if (static_cast<uint64_t>(time(nullptr)) > it->second.second) return false;
        addr = it->second.first;
        return true;
    }
    return false;
}

void MasterClientBase::RemoveCache(const std::string &key) {
    addr_cache_.erase(key);
}


}  // namespace comm

}  // namespace phxqueue


//gzrd_Lib_CPP_Version_ID--start
#ifndef GZRD_SVN_ATTR
#define GZRD_SVN_ATTR "0"
#endif
static char gzrd_Lib_CPP_Version_ID[] __attribute__((used))="$HeadURL: http://scm-gy.tencent.com/gzrd/gzrd_mail_rep/phoenix_proj/trunk/phoenix/phxlogic/masterclient/phxmasterclient.cpp $ $Id: phxmasterclient.cpp 1810555 2016-11-04 12:39:41Z walnuthe $ " GZRD_SVN_ATTR "__file__";
// gzrd_Lib_CPP_Version_ID--end

