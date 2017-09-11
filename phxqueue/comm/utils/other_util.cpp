/*
Tencent is pleased to support the open source community by making PhxQueue available.
Copyright (C) 2017 THL A29 Limited, a Tencent company. All rights reserved.
Licensed under the BSD 3-Clause License (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at

<https://opensource.org/licenses/BSD-3-Clause>

Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.
*/



#include "phxqueue/comm/utils/other_util.h"

#include <cmath>
#include <cstdio>
#include <cstdlib>
#include <cstdlib>
#include <errno.h>
#include <pthread.h>
#include <sys/time.h>
#include <time.h>
#include <unistd.h>


namespace phxqueue {

namespace comm {

namespace utils {


#ifdef __i386

__inline__ uint64_t rdtsc() {
    uint64_t x;
    __asm__ volatile ("rdtsc" : "=A" (x));
    return x;
}

#elif __amd64

__inline__ uint64_t rdtsc() {

    uint64_t a, d;
    __asm__ volatile ("rdtsc" : "=a" (a), "=d" (d));
    return (d<<32) | a;
}

#endif

struct FastRandomSeed {
    bool init;
    unsigned int seed;
};

static thread_local FastRandomSeed seed_thread_safe = { false, 0 };

static void ResetFastRandomSeed() {
    seed_thread_safe.seed = rdtsc();
    seed_thread_safe.init = true;
}

static void InitFastRandomSeedAtFork() {
    pthread_atfork(ResetFastRandomSeed, ResetFastRandomSeed, ResetFastRandomSeed);
}

static void InitFastRandomSeed() {
    static pthread_once_t once = PTHREAD_ONCE_INIT;
    pthread_once(&once, InitFastRandomSeedAtFork);

    ResetFastRandomSeed();
}

const uint32_t OtherUtils::FastRand() {
    if (!seed_thread_safe.init) {
        InitFastRandomSeed();
    }

    return rand_r(&seed_thread_safe.seed);
}


}  // namespace utils

}  // namespace comm

}  // namespace phxqueue

