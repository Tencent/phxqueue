/*
Tencent is pleased to support the open source community by making PhxQueue available.
Copyright (C) 2017 THL A29 Limited, a Tencent company. All rights reserved.
Licensed under the BSD 3-Clause License (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at

<https://opensource.org/licenses/BSD-3-Clause>

Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.
*/



#include "phxqueue_phxrpc/producer.h"

#include "phxqueue_phxrpc/app/store/store_client.h"

namespace phxqueue_phxrpc {

namespace producer {

using namespace std;

Producer::Producer(const phxqueue::producer::ProducerOption &opt)
        : phxqueue::producer::Producer(opt) {}

Producer::~Producer() {}

void Producer::CompressBuffer(const string &buffer, string &compressed_buffer, int &buffer_type) {
    compressed_buffer = buffer;
    buffer_type = 0;
}

phxqueue::comm::RetCode Producer::Add(const phxqueue::comm::proto::AddRequest &req, phxqueue::comm::proto::AddResponse &resp) {
    StoreClient store_client;
    auto ret = store_client.ProtoAdd(req, resp);

    if (phxqueue::comm::RetCode::RET_OK != ret) {
        QLErr("ProtoAdd ret %d", phxqueue::comm::as_integer(ret));
    }
    return ret;
}


}  // namespace producer

}  // namespace phxqueue_phxrpc

