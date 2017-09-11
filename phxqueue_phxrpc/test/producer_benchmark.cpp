/*
Tencent is pleased to support the open source community by making PhxQueue available.
Copyright (C) 2017 THL A29 Limited, a Tencent company. All rights reserved.
Licensed under the BSD 3-Clause License (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at

<https://opensource.org/licenses/BSD-3-Clause>

Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.
*/



#include "phxqueue_phxrpc/test/producer_benchmark.h"


namespace phxqueue_phxrpc {

namespace test {


using namespace std;


ProducerBenchMark::ProducerBenchMark(const int qps, const int nthread, const int nroutine,
                                     const int buf_size, const int ndaemon_batch_thread)
        : phxqueue::comm::utils::BenchMark(qps, nthread, nroutine) {
    buf_ = string(buf_size ? buf_size : 1, 'a');

    phxqueue::producer::ProducerOption opt;
    opt.ndaemon_batch_thread = ndaemon_batch_thread;
    producer_.reset(new phxqueue_phxrpc::producer::Producer(opt));
    producer_->Init();
}

int ProducerBenchMark::TaskFunc(const int vtid) {

    phxqueue::comm::RetCode ret;

    const int topic_id{1000};
    const uint64_t uin{0};
    const int handle_id{1};
    const int pub_id{1};

    ret = producer_->Enqueue(topic_id, uin, handle_id, buf_, pub_id);

    return phxqueue::comm::as_integer(ret);
}


}  // namespace test

}  // namespace phxqueue_phxrpc

