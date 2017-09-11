#include <iostream>
#include <set>

#include "phxqueue/comm.h"
#include "phxqueue/plugin.h"
#include "phxqueue/config.h"
#include "phxqueue/consumer.h"

using namespace phxqueue;
using namespace std;

int GetStoreIDAndQueueIDPairsByPubID(const int topic_id, const int pub_id, set<pair<int, int>> &pairs) {
    comm::RetCode ret;

    pairs.clear();

    shared_ptr<const config::TopicConfig> topic_config;
    if (comm::RetCode::RET_OK != (ret = config::GlobalConfig::GetThreadInstance()->GetTopicConfigByTopicID(topic_id, topic_config))) {
        NLErr("GetTopicConfigByTopicID ret %d topic_id %d", as_integer(ret), topic_id);
        return -1;
    }

    shared_ptr<const config::StoreConfig> store_config;
    if (comm::RetCode::RET_OK != (ret = config::GlobalConfig::GetThreadInstance()->GetStoreConfig(topic_id, store_config))) {
        NLErr("GetStoreConfig ret %d", as_integer(ret));
        return -1;
    }

    set<int> store_ids;
    if (comm::RetCode::RET_OK != (ret = store_config->GetAllStoreID(store_ids))) {
        NLErr("GetAllStoreID ret %d", as_integer(ret));
        return -1;
    }

    shared_ptr<const config::proto::Pub> pub;
    if (comm::RetCode::RET_OK != (ret = topic_config->GetPubByPubID(pub_id, pub))) {
        NLErr("GetPubByPubID ret %d pub_id %d", as_integer(ret), pub_id);
        return -1;
    }

    vector<unique_ptr<consumer::Queue_t>> queues;
    for (int i{0}; i < pub->queue_info_ids_size(); ++i) {
        auto queue_info_id = pub->queue_info_ids(i);

        set<int> queue_ids;
        if (comm::RetCode::RET_OK != (ret = topic_config->GetQueuesByQueueInfoID(queue_info_id, queue_ids))) {
            NLErr("GetSubIDsByPubID ret %d pub_id %d", as_integer(ret), pub_id);
            continue;
        }

        for (auto store_id : store_ids) {
            for (auto queue_id : queue_ids) {
                pairs.insert(make_pair(store_id, queue_id));
            }
        }
    }
    return 0;
}


int main(int argc, char **argv) {
    comm::LogFunc log_func;
    plugin::LoggerGoogle::GetLogger("test_config", "/tmp/phxqueue/log", 3, log_func);
    comm::Logger::GetInstance()->SetLogFunc(log_func);

    

    const int topic_id = 1000;
    const int pub_id = 1;
    set<pair<int, int>> pairs;
    int ret = GetStoreIDAndQueueIDPairsByPubID(topic_id, pub_id, pairs);
    printf("ret %d\n", ret);
    for (auto &&p : pairs) {
        printf("store %d queue %d\n", p.first, p.second);
    }
    return 0;
}

//gzrd_Lib_CPP_Version_ID--start
#ifndef GZRD_SVN_ATTR
#define GZRD_SVN_ATTR "0"
#endif
static char gzrd_Lib_CPP_Version_ID[] __attribute__((used))="$HeadURL$ $Id$ " GZRD_SVN_ATTR "__file__";
// gzrd_Lib_CPP_Version_ID--end

