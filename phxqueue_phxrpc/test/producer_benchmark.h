#pragma once

#include <memory>
#include <string>

#include "phxqueue/comm.h"
#include "phxqueue_phxrpc/producer.h"


namespace phxqueue_phxrpc {

namespace test {


class ProducerBenchMark : public phxqueue::comm::utils::BenchMark {
  public:
    ProducerBenchMark(const int qps, const int nthread, const int nroutine,
                      const int buf_size, const int ndaemon_batch_thread = 0);
    virtual ~ProducerBenchMark() override = default;
    int TaskFunc(const int vtid);

  private:
    std::string buf_;
    std::unique_ptr<phxqueue::producer::Producer> producer_;
};


}  // namespace test

}  // namespace phxqueue_phxrpc

