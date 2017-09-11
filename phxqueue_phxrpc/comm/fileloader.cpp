/*
Tencent is pleased to support the open source community by making PhxQueue available.
Copyright (C) 2017 THL A29 Limited, a Tencent company. All rights reserved.
Licensed under the BSD 3-Clause License (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at

<https://opensource.org/licenses/BSD-3-Clause>

Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.
*/



#include "phxqueue_phxrpc/comm/fileloader.h"

#include <algorithm>
#include <assert.h>
#include <chrono>
#include <cinttypes>
#include <cmath>
#include <cstdio>
#include <cstring>
#include <errno.h>
#include <iostream>
#include <pthread.h>
#include <stdarg.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>
#include <vector>


namespace phxqueue_phxrpc {

namespace comm {


using namespace std;


class FileLoader::FileLoaderImpl {
  public:
    FileLoaderImpl() {}
    virtual ~FileLoaderImpl() {}

    string file;
    int check_interval_s = 60;
    int refresh_delay_s = 60;
    uint64_t last_check_time = 0;
    uint64_t last_mod_time = 0;
    ino_t ino;
    pthread_rwlock_t rwlock;
};

FileLoader::FileLoader() : impl_(new FileLoaderImpl()) {
    assert(impl_);
    pthread_rwlock_init(&impl_->rwlock, NULL);
}

FileLoader::FileLoader(const string &file) : impl_(new FileLoaderImpl()) {
    assert(impl_);
    impl_->file = file;
    pthread_rwlock_init(&impl_->rwlock, NULL);
}

FileLoader::~FileLoader() {
}

void FileLoader::SetCheckInterval(const int check_interval_s) {
    impl_->check_interval_s = check_interval_s;
}

void FileLoader::SetRefreshDelay(const int refresh_delay_s) {
    impl_->refresh_delay_s = refresh_delay_s;
}

bool FileLoader::SetFileIfUnset(const std::string &file) {
    if (impl_->file.empty()) {
        impl_->file = file;
        return true;
    }
    return false;
}

void FileLoader::ResetTimeByInode(ino_t ino) {
    if (impl_->ino != ino) {
        impl_->ino = ino;
        impl_->last_mod_time = 0;
    }
}

bool FileLoader::LoadIfModified() {
    bool is_modified = false;
    return LoadIfModified(is_modified);
}

bool FileLoader::LoadIfModified(bool &is_modified) {
    if (impl_->file.empty()) {
        return false;
    }

    time_t now_time = time(nullptr);
    is_modified = false;

    bool ret = true;

    if (impl_->last_check_time > now_time + 30ULL) {
        impl_->last_check_time = now_time;
    }

    if (impl_->last_mod_time > now_time + 30ULL) {
        impl_->last_mod_time = 0;
    }

    if (static_cast<uint64_t>(now_time) > impl_->last_check_time + impl_->check_interval_s) {
        struct stat fileStat;

        if (0 == stat(impl_->file.c_str(), &fileStat)) {
            ResetTimeByInode(fileStat.st_ino);

            int delay = std::abs(int64_t(now_time - fileStat.st_mtime));
            int interval = std::abs(int64_t(fileStat.st_mtime - impl_->last_mod_time));
            if (0 == impl_->last_mod_time
                || (interval > 0 && delay > impl_->refresh_delay_s)) {
                assert(0 == pthread_rwlock_wrlock(&impl_->rwlock));

                // double check
                if (static_cast<uint64_t>(now_time) > impl_->last_check_time + impl_->check_interval_s) {
                    is_modified = true;

                    bool load_ok = LoadFile(impl_->file, impl_->last_mod_time > 0);
                    if (load_ok) {
                        impl_->last_check_time = now_time;	// for double check in other threads
                        impl_->last_mod_time = fileStat.st_mtime;
                    } else {
                        //QLErr("LoadFile fail");
                    }
                }

                assert(0 == pthread_rwlock_unlock(&impl_->rwlock));
            }
        }
        else {
            ret = false;
        }
        impl_->last_check_time = now_time;
    }

    return ret;
}


uint64_t FileLoader::GetLastModTime() {
    return impl_->last_mod_time;
}


}  // namespace comm

}  // namespace phxqueue_phxrpc

