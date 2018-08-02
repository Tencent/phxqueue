/*
Tencent is pleased to support the open source community by making PhxQueue available.
Copyright (C) 2017 THL A29 Limited, a Tencent company. All rights reserved.
Licensed under the BSD 3-Clause License (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at

<https://opensource.org/licenses/BSD-3-Clause>

Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.
*/



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
            NLErr("GetConsumerGroupIDsByPubID ret %d pub_id %d", as_integer(ret), pub_id);
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

