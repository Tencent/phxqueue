#pragma once

#include <cassert>
#include <string>
#include <vector>


namespace phxqueue {

namespace comm {

namespace utils {


class MutexGuard {
  public:
    MutexGuard(const MutexGuard &) = delete;
    MutexGuard(MutexGuard &&) = delete;
    MutexGuard(pthread_mutex_t *mutex) {
        mutex_ = mutex;
        //NLVerb("before mutex");
        pthread_mutex_lock(mutex_);
    }
    virtual ~MutexGuard() {
        if (mutex_) pthread_mutex_unlock(mutex_);
        //NLVerb("after mutex");
    }

  private:
    pthread_mutex_t *mutex_{nullptr};
};

class RWLock {
  public:
    enum class LockMode {
        READ = 1,
        WRITE = 2
    };

    explicit RWLock(pthread_rwlock_t *l, LockMode mode) : l_(l) {
        if (LockMode::READ == mode) {
            assert(0 == pthread_rwlock_rdlock(l_));
        } else if (LockMode::WRITE == mode) {
            assert(0 == pthread_rwlock_wrlock(l_));
        }
    }

    virtual ~RWLock() {
        assert(0 == pthread_rwlock_unlock(l_));
    }

    RWLock(const RWLock &) = delete;
    RWLock &operator=(const RWLock &) = delete;

  private:
    pthread_rwlock_t *const l_;
};


}  // namespace utils

}  // namespace comm

}  // namespace phxqueue

