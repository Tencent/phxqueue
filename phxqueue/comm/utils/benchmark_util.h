#pragma once


#include <thread>
#include <map>
#include <vector>
#include <memory>

namespace phxqueue {

namespace comm {

namespace utils {

class BenchMark {
  public:
    BenchMark(const int qps, const int nthread, const int nroutine);

    virtual ~BenchMark();
    virtual int TaskFunc(const int vtid) = 0;
    virtual void BeforeThreadRun() {};

    void Run();
    int GetRoutineSleepTimeMS();
    void ThreadRun(const int vtid);
    void Stat(const int vtid, const int ret, const uint64_t used_time_ms, const int sleep_ms);


  private:
    void ResStat();

  private:
    class BenchMarkImpl;
    std::unique_ptr<BenchMarkImpl> impl_;
};


}  // namespace utils

}  // namespace comm

}  // namespace phxqueue

