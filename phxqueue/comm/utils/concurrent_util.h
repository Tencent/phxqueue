/*
Tencent is pleased to support the open source community by making PhxQueue available.
Copyright (C) 2017 THL A29 Limited, a Tencent company. All rights reserved.
Licensed under the BSD 3-Clause License (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at

<https://opensource.org/licenses/BSD-3-Clause>

Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.
*/



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

