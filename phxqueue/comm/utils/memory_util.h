/*
Tencent is pleased to support the open source community by making PhxQueue available.
Copyright (C) 2017 THL A29 Limited, a Tencent company. All rights reserved.
Licensed under the BSD 3-Clause License (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at

<https://opensource.org/licenses/BSD-3-Clause>

Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.
*/



#include <memory>


namespace phxqueue {

namespace comm {

namespace utils {


template <typename T, typename... Ts>
std::unique_ptr<T> make_unique(Ts&&... params) {
    return std::unique_ptr<T>(new T(std::forward<T>(params)...));
}


class MemStat {
  public:
    MemStat() = default;
    virtual ~MemStat() = default;


    unsigned long size{0};  // total program size
    unsigned long resident{0};  // resident set size
    unsigned long share{0};  // shared pages
    unsigned long text{0};  // text (code)
    unsigned long lib{0};  // library
    unsigned long data{0};  // data/stack
    unsigned long dt{0};  // dirty pages

    bool Stat(pid_t pid = 0);
};


}  // namespace utils

}  // namespace comm

}  // namespace phxqueue

