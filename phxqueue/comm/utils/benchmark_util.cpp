#include <cstdio>
#include <cstring>
#include <poll.h>
#include <sys/time.h>
#include <thread>

#include "phxqueue/comm/utils/benchmark_util.h"
#include "phxqueue/comm/utils/concurrent_util.h"
#include "phxqueue/comm/utils/time_util.h"

#include "co_routine.h"


namespace phxqueue {

namespace comm {

namespace utils {


using namespace std;


struct CoWorkerCtx_t {
    stCoRoutine_t *co;
    int vtid;
    int rid;
    BenchMark *bm;
};

struct ThreadRunFuncArgs_t {
    int vtid;
    BenchMark *bm;
};

static uint64_t DiffTimespecMS(const struct timespec &st, const struct timespec &ed)
{
    return (ed.tv_sec - st.tv_sec) * 1000 + ed.tv_nsec / 1000000 - st.tv_nsec / 1000000;
}

static void *ThreadRunFunc(void *void_args) {
    ThreadRunFuncArgs_t *args{reinterpret_cast<ThreadRunFuncArgs_t *>(void_args)};
    args->bm->BeforeThreadRun();
    args->bm->ThreadRun(args->vtid);
    return nullptr;
}

static void *RoutineRunFunc(void *arg) {
    co_enable_hook_sys();

    CoWorkerCtx_t *ctx = static_cast<CoWorkerCtx_t*>(arg);
    BenchMark *bm = ctx->bm;
    const int vtid = ctx->vtid;

    PoissonDistribution pd(bm->GetRoutineSleepTimeMS());
    uint64_t sleep_ms;
    int ret;
    uint64_t used_time_ms{0};
    struct timespec t1, t2;

    while (1) {
        sleep_ms = pd.GetNextIntervalTimeMs();
        if (sleep_ms > used_time_ms) sleep_ms -= used_time_ms;
        else sleep_ms = 0;
        if (sleep_ms) poll(nullptr, 0, sleep_ms);

        clock_gettime(CLOCK_MONOTONIC, &t1);
        ret = bm->TaskFunc(vtid);

        clock_gettime(CLOCK_MONOTONIC, &t2);
        used_time_ms = DiffTimespecMS(t1, t2);

        bm->Stat(vtid, ret, used_time_ms, sleep_ms);
    }

    return NULL;
}

class StatData {
public:
    StatData() {
        pthread_mutex_init(&mutex, nullptr);
    }
    pthread_mutex_t mutex;
    std::map<int, uint32_t> res_st;
    std::map<uint64_t, uint32_t> time_st;
    int nroutine_sleep = 0;
};

class BenchMark::BenchMarkImpl {
  public:
    timeval last_stat_time;
    int qps = 0;
    int nthread = 0;
    int nroutine = 0;
    int routine_sleep_time_ms = 0;
    std::unique_ptr<StatData[]> stats;
};

BenchMark::BenchMark(const int qps, const int nthread, const int nroutine) : impl_(new BenchMarkImpl()) {
    impl_->qps = qps < 1 ? 1 : qps;
    impl_->nthread = nthread < 1 ? 1 : nthread;
    impl_->nroutine = nroutine < 1 ? 1 : nroutine;
    impl_->routine_sleep_time_ms = (int)(1000.0 / (1.0 * impl_->qps / impl_->nthread / impl_->nroutine));
}

BenchMark::~BenchMark() {}


void BenchMark::Stat(const int vtid, const int ret, const uint64_t used_time_ms, const int sleep_ms) {


    StatData *stat = &impl_->stats[vtid];
    MutexGuard guard(&stat->mutex);

    ++stat->res_st[ret];
    ++stat->time_st[used_time_ms];
    if (sleep_ms) ++stat->nroutine_sleep;
}

void BenchMark::ThreadRun(const int vtid) {
    stCoRoutineAttr_t attr;
    attr.stack_size = 1024 * 16;


    std::unique_ptr<struct CoWorkerCtx_t[]> ctxs(new struct CoWorkerCtx_t[impl_->nroutine]);

    for (int i{0}; i < impl_->nroutine; ++i)
    {
        CoWorkerCtx_t *ctx = &ctxs[i];
        ctx->co = nullptr;
        ctx->bm = this;
        ctx->vtid = vtid;
        ctx->rid = i;

        co_create(&(ctx->co), &attr, RoutineRunFunc, ctx);
        co_resume(ctx->co);
    }
    co_eventloop(co_get_epoll_ct(), nullptr, this);
}

void BenchMark::Run()
{
    printf("qps: %d\n", impl_->qps);
    printf("nthread: %d\n", impl_->nthread);
    printf("nroutine: %d\n", impl_->nroutine);
    printf("routine_sleep_time(ms): %d\n", impl_->routine_sleep_time_ms);
    printf("start run...\n");

    impl_->stats.reset(new StatData[impl_->nthread]);

    for (int i{0}; i < impl_->nthread; ++i) {
        pthread_t tid;
        pthread_create(&tid, 0, ThreadRunFunc, new ThreadRunFuncArgs_t{i, this});
        pthread_detach(tid);
    }

    // stat
    gettimeofday(&impl_->last_stat_time, nullptr);
    while (true) {
        std::this_thread::sleep_for(std::chrono::seconds(10));
        ResStat();
    }
}

int BenchMark::GetRoutineSleepTimeMS() {
    return impl_->routine_sleep_time_ms;
}



void BenchMark::ResStat() {
    timeval now;
    gettimeofday(&now, nullptr);
    long diff_ms{(now.tv_sec - impl_->last_stat_time.tv_sec) * 1000 + (now.tv_usec - impl_->last_stat_time.tv_usec) / 1000};
    impl_->last_stat_time = now;

    const uint32_t time_split[] = {1, 2, 5, 10, 20, 50, 100, 200, 500, 1000};
    const int time_split_size = sizeof(time_split) / sizeof(int);
    int time_split_cnt[sizeof(time_split) / sizeof(int) + 1];
    memset(time_split_cnt, 0, sizeof(time_split_cnt));

    map<int, uint32_t> m;
    uint32_t total{0};
    uint64_t total_time_ms{0};
    uint32_t nroutine_sleep{0};
    for (int i{0}; i < impl_->nthread; ++i) {
        StatData *stat = &impl_->stats[i];
        MutexGuard guard(&stat->mutex);

        for (auto &&it : stat->res_st) {
            m[it.first] += it.second;
            total += it.second;
        }
        stat->res_st.clear();

        for (auto &&it : stat->time_st) {
            int j{0};
            for (; j < time_split_size; ++j)
                if (it.first < time_split[j]) break;
            time_split_cnt[j] += it.second;
            total_time_ms += (uint64_t)it.first * it.second;
        }
        stat->time_st.clear();

        nroutine_sleep += stat->nroutine_sleep;
        stat->nroutine_sleep = 0;
    }

    // log
    printf("\n-------------------------------------------\n");
    printf("--\ttotal:\t%u\n", total);
    printf("--\ttime(ms):\t%ld\n", diff_ms);
    printf("--\tqps:\t%.2lf\n", (double)total / diff_ms * 1000);
    printf("--\troutine_sleep:\t%.2lf%%\n", (double)nroutine_sleep / total * 100.0);
    printf("--\tretcode\tcnt\tpercent\n");
    for (auto it : m) {
        printf("--\t%d\t%u\t%.2lf\n", it.first, it.second, (double)it.second / total * 100.0);
    }

    printf("--\tusetime(ms)\tcnt\tpercent\n");
    for (int i{0}; i < time_split_size; ++i) {
        printf("--\t< %d\t\t%u\t%.2lf\n", time_split[i], time_split_cnt[i], (double)time_split_cnt[i] / total * 100.0);
    }
    printf("--\t>= %d\t\t%u\t%.2lf\n", time_split[time_split_size - 1], time_split_cnt[time_split_size], (double)time_split_cnt[time_split_size] / total * 100.0);
	printf("--\tavg_usetime(ms):\t%.2lf\n", (double)total_time_ms / total);
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

