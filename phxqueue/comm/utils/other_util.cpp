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


//gzrd_Lib_CPP_Version_ID--start
#ifndef GZRD_SVN_ATTR
#define GZRD_SVN_ATTR "0"
#endif
static char gzrd_Lib_CPP_Version_ID[] __attribute__((used))="$HeadURL$ $Id$ " GZRD_SVN_ATTR "__file__";
// gzrd_Lib_CPP_Version_ID--end

