/*
Tencent is pleased to support the open source community by making PhxQueue available.
Copyright (C) 2017 THL A29 Limited, a Tencent company. All rights reserved.
Licensed under the BSD 3-Clause License (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at

<https://opensource.org/licenses/BSD-3-Clause>

Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.
*/



#pragma once

#include <memory>
#include <string>
#include <sys/stat.h>


namespace phxqueue_phxrpc {

namespace comm {


class FileLoader {
  public:
    FileLoader();
    FileLoader(const std::string &file);
    virtual ~FileLoader();

    void SetCheckInterval(const int check_interval_s);
    void SetRefreshDelay(const int refresh_delay_s);
    bool SetFileIfUnset(const std::string &file);

    void ResetTimeByInode(ino_t ino);
    bool LoadIfModified();
    bool LoadIfModified(bool &is_modified);
    uint64_t GetLastModTime();

    virtual bool LoadFile(const std::string &file, bool is_reload) = 0;

  private:
    class FileLoaderImpl;
    std::unique_ptr<FileLoaderImpl> impl_;
};


}  // namespace comm

}  // namespace phxqueue_phxrpc

