/*
Tencent is pleased to support the open source community by making PhxQueue available.
Copyright (C) 2017 THL A29 Limited, a Tencent company. All rights reserved.
Licensed under the BSD 3-Clause License (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at

<https://opensource.org/licenses/BSD-3-Clause>

Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.
*/



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

