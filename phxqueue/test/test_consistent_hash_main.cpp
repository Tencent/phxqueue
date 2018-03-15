/*
Tencent is pleased to support the open source community by making PhxQueue available.
Copyright (C) 2017 THL A29 Limited, a Tencent company. All rights reserved.
Licensed under the BSD 3-Clause License (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at

<https://opensource.org/licenses/BSD-3-Clause>

Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.
*/



#include <iostream>
#include <list>

#include "phxqueue/comm.h"

using namespace phxqueue;
using namespace std;

int main(int argc, char **argv) {

    comm::utils::ConsistenHash<int, int> consisten_hash;

    std::list<comm::utils::ConsistenHash<int, int>::NodeScale> node_scale_list;
    node_scale_list.push_back(std::pair<int, int>(1, 10));
    node_scale_list.push_back(std::pair<int, int>(2, 10));

    bool ret;
    ret = consisten_hash.Init(node_scale_list,
                        [](const int &key)->uint64_t {
                            uint64_t h = 0;
                            h = comm::utils::MurmurHash64(&key, sizeof(int), h);
                            return h;
                        },
                        [](const int &node, int scale)->uint64_t {
                            uint64_t h = 0;
                            h = comm::utils::MurmurHash64(&node, sizeof(int), h);
                            h = comm::utils::MurmurHash64(&scale, sizeof(int), h);
                            return h;
                        });
    printf("Init ret %d\n", ret);


    for (int i{0}; i < 100; ++i) {
        int key = 5564925 + i;
        int node = 0;
        ret = consisten_hash.PickNodeByKey(key, node);
        printf("i %d ret %d node %d\n", i, ret, node);
    }
    return 0;
}

