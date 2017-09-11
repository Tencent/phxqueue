/*
Tencent is pleased to support the open source community by making PhxQueue available.
Copyright (C) 2017 THL A29 Limited, a Tencent company. All rights reserved.
Licensed under the BSD 3-Clause License (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at

<https://opensource.org/licenses/BSD-3-Clause>

Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.
*/



#pragma once

#include <cstdint>
#include <cstdio>
#include <random>
#include <ctime>
#include <iostream>
#include <memory>

namespace phxqueue {

namespace comm {

namespace utils {


class Time {
  public:
    static const uint64_t GetTimestampMS();

    static const uint64_t GetSteadyClockMS();

    static void MsSleep(const int time_ms);
};


class PoissonDistribution {
public:
	PoissonDistribution();
	
	PoissonDistribution(const int base_interval_time_ms);
	
	~PoissonDistribution();

	static PoissonDistribution * GetInstance();

	//珀松分布下一个时间间隔
	//获取后可自行处理这个间隔
	const int GetNextIntervalTimeMs();

	//直接进入下一个时间间隔的睡眠
	//进入函数后会一直睡眠完整个interval的时间
	//可通过其他线程调用RealtimeChangeBaseIntervalMs唤醒
	//返回值是这次sleep的时间
	int NextIntervalSleepMs();

	//实时改变基准间隔
	//该函数做了处理防止泄洪效应
	void RealtimeChangeBaseIntervalMs(const int new_base_interval_ms);

	//设置最大的珀松分布后的间隔时间
	void SetMaxIntervalTimeMs(const int max_interval_time_ms);

	//设置最小的珀松分布后的间隔时间
	void SetMinIntervalTimeMs(const int min_interval_time_ms);

	void MsSleep(const int time_ms);

private:
	void Rebuild(const int base_interval_time_ms);

private:
    class PoissonDistributionImpl;
    std::unique_ptr<PoissonDistributionImpl> impl_;
};


}  // namespace utils

}  // namespace comm

}  // namespace phxqueue

