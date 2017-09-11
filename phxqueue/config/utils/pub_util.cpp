#include <string>

#include "phxqueue/comm.h"

#include "phxqueue/config/globalconfig.h"
#include "phxqueue/config/storeconfig.h"
#include "phxqueue/config/topicconfig.h"


namespace phxqueue {

namespace config {

namespace utils {


using namespace std;


comm::RetCode GetPubIDsByStoreID(const int topic_id, const int store_id, set<int> &pub_ids) {
    pub_ids.clear();

    comm::RetCode ret;

    shared_ptr<const StoreConfig> store_config;
    if (comm::RetCode::RET_OK != (ret = GlobalConfig::GetThreadInstance()->GetStoreConfig(topic_id, store_config))) {
        NLErr("GetStoreConfig ret %d topic_id %d", comm::as_integer(ret), topic_id);
        return ret;
    }

    shared_ptr<const proto::Store> store;
    if (comm::RetCode::RET_OK != (ret = store_config->GetStoreByStoreID(store_id, store))) {
        NLErr("GetStoreByStoreID ret %d", comm::as_integer(ret));
        return ret;
    }

    shared_ptr<const TopicConfig> topic_config;
    if (comm::RetCode::RET_OK != (ret = GlobalConfig::GetThreadInstance()->GetTopicConfigByTopicID(topic_id, topic_config))) {
        NLErr("GetTopicConfig ret %d topic_id %d", comm::as_integer(ret), topic_id);
        return ret;
    }

    if (store->pub_ids_size()) {
        for (int i{0}; i < store->pub_ids_size(); ++i) {
            auto &&pub_id = store->pub_ids(i);
            if (topic_config->IsValidPubID(pub_id)) {
                pub_ids.insert(store->pub_ids(i));
            }
        }
        return comm::RetCode::RET_OK;
    }

    if (comm::RetCode::RET_OK != (ret = topic_config->GetAllPubID(pub_ids))) {
        NLErr("GetAllPubID ret %d", comm::as_integer(ret));
    }

    return ret;
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

