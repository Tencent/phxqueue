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

