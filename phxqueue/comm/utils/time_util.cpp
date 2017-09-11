/*
Tencent is pleased to support the open source community by making PhxQueue available.
Copyright (C) 2017 THL A29 Limited, a Tencent company. All rights reserved.
Licensed under the BSD 3-Clause License (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at

<https://opensource.org/licenses/BSD-3-Clause>

Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.
*/



#include "phxqueue/comm/utils/time_util.h"

#include <chrono>
#include <errno.h>
#include <unistd.h>
#include <assert.h>

namespace phxqueue {

namespace comm {

namespace utils {


using namespace std;


const uint64_t Time::GetTimestampMS() {
    auto now_time = chrono::system_clock::now();
    uint64_t now = (chrono::duration_cast<chrono::milliseconds>(now_time.time_since_epoch())).count();
    return now;
}

const uint64_t Time::GetSteadyClockMS()  {
    auto now_time = chrono::steady_clock::now();
    uint64_t now = (chrono::duration_cast<chrono::milliseconds>(now_time.time_since_epoch())).count();
    return now;
}

void Time::MsSleep(const int time_ms) {
    timespec t;
    t.tv_sec = time_ms / 1000;
    t.tv_nsec = (time_ms % 1000) * 1000000;

    int ret = 0;
    do {
        ret = ::nanosleep(&t, &t);
    } while (ret == -1 && errno == EINTR);
}

class PoissonDistribution::PoissonDistributionImpl {
public:
    std::unique_ptr<std::exponential_distribution<double>> delay_distribution;
    std::unique_ptr<std::default_random_engine> delay_generator;

	int last_base_interval_time_ms = 0;
	int min_interval_time_ms = 0;
	int max_interval_time_ms = 0;

	bool base_interval_time_change = false;
};

PoissonDistribution :: PoissonDistribution() : impl_(new PoissonDistributionImpl()) {}

PoissonDistribution :: PoissonDistribution(const int base_interval_time_ms) : impl_(new PoissonDistributionImpl()) {
	impl_->last_base_interval_time_ms = base_interval_time_ms;

	Rebuild(impl_->last_base_interval_time_ms);
}

PoissonDistribution :: ~PoissonDistribution() {}


PoissonDistribution * PoissonDistribution :: GetInstance() {
	static PoissonDistribution pd;
	return &pd;
}

void PoissonDistribution :: Rebuild(const int base_interval_time_ms) {
    double times_per_ms = (double)1.0 / (double)base_interval_time_ms;
    impl_->delay_distribution.reset(new std::exponential_distribution<double>(times_per_ms));
	assert(impl_->delay_distribution != nullptr);

    int seed = std::chrono::system_clock::now().time_since_epoch().count();
    impl_->delay_generator.reset(new std::default_random_engine(seed));
	assert(impl_->delay_generator != nullptr);
}

const int PoissonDistribution :: GetNextIntervalTimeMs() {

	if (impl_->last_base_interval_time_ms == 0) {
		return 0;
	}

	if (impl_->base_interval_time_change) {
		impl_->base_interval_time_change = false;
		
		Rebuild(impl_->last_base_interval_time_ms);
	}

	int next_interval_time_ms = (int)((*impl_->delay_distribution)(*impl_->delay_generator));
	if (impl_->min_interval_time_ms > 0 && next_interval_time_ms < impl_->min_interval_time_ms) {
		next_interval_time_ms = impl_->min_interval_time_ms;
	}
	else if (impl_->max_interval_time_ms > 0 && next_interval_time_ms > impl_->max_interval_time_ms) {
		next_interval_time_ms = impl_->max_interval_time_ms;
	}

	return next_interval_time_ms;
}

void PoissonDistribution :: RealtimeChangeBaseIntervalMs(const int new_base_interval_ms)
{
	if (new_base_interval_ms != impl_->last_base_interval_time_ms) {
		impl_->last_base_interval_time_ms = new_base_interval_ms;
		impl_->base_interval_time_change = true;
	}
}

int PoissonDistribution :: NextIntervalSleepMs()
{
    int already_sleep_ms = 0;
    int need_sleep_ms = GetNextIntervalTimeMs();
	int all_sleep_ms = 0;

    while(need_sleep_ms > 0) {
        int need_sleep_ms_this_round = need_sleep_ms > 1000 ? 1000 : need_sleep_ms;
        need_sleep_ms -= need_sleep_ms_this_round;
        already_sleep_ms += need_sleep_ms_this_round;

		all_sleep_ms += need_sleep_ms_this_round;
		
		//分段sleep，在base改变的时候及时做出响应
		MsSleep(need_sleep_ms_this_round);

        if (impl_->base_interval_time_change)
        {
            int new_next_interval_time_ms = GetNextIntervalTimeMs();
            if (new_next_interval_time_ms == 0)
            {
				return all_sleep_ms;
            }

            need_sleep_ms = new_next_interval_time_ms > already_sleep_ms ?
                new_next_interval_time_ms - already_sleep_ms : 0;

            if (need_sleep_ms == 0)
            {
				//如果最新设置的base太小，会导致集体退出sleep从而导致泄洪效应
				//这时候立即开始一次新的interval
				already_sleep_ms = 0;
				need_sleep_ms = GetNextIntervalTimeMs();
            }
        }
    }

	return all_sleep_ms;
}

void PoissonDistribution :: SetMaxIntervalTimeMs(const int max_interval_time_ms)
{
	impl_->max_interval_time_ms = max_interval_time_ms;
}

void PoissonDistribution :: SetMinIntervalTimeMs(const int min_interval_time_ms)
{
	impl_->min_interval_time_ms = min_interval_time_ms;
}

void PoissonDistribution :: MsSleep(const int time_ms)
{
    timespec t;
    t.tv_sec = time_ms / 1000; 
    t.tv_nsec = (time_ms % 1000) * 1000000;

    int ret = 0;
    do 
	{
		ret = ::nanosleep(&t, &t);
    } while (ret == -1 && errno == EINTR); 
}

}  // namespace utils

}  // namespace comm

}  // namespace phxqueue

