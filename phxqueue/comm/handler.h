/*
Tencent is pleased to support the open source community by making PhxQueue available.
Copyright (C) 2017 THL A29 Limited, a Tencent company. All rights reserved.
Licensed under the BSD 3-Clause License (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at

<https://opensource.org/licenses/BSD-3-Clause>

Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.
*/



#pragma once

#include "phxqueue/comm/proto/comm.pb.h"


namespace phxqueue {

namespace comm {


enum class HandleResult {
    RES_OK = 0,
    RES_ERROR = -2,
    RES_IGNORE = -3,
};

class Handler {
  public:
    Handler() {}
    virtual ~Handler() {}

    virtual HandleResult Handle(const proto::ConsumerContext &cc, proto::QItem &item, std::string &uncompressed_buffer) = 0;

    void SetBufferUpdated(bool buffer_updated) {buffer_updated_ = buffer_updated;}
    bool IsBufferUpdated() { return buffer_updated_; }

  private:
    bool buffer_updated_{false};
};

template <class T>
class PBHandler : public Handler {
  public:
    PBHandler() {}
    virtual ~PBHandler() {}

    virtual HandleResult Handle(const proto::ConsumerContext &cc, proto::QItem &item, std::string &uncompressed_buffer) {
        T req;
        if (!req.ParseFromString(uncompressed_buffer)) {
            return HandleResult::RES_ERROR;
        }
        auto result = HandleRequest(item.meta().uin(), cc, *item.mutable_sys_cookies(), req);
        if (IsBufferUpdated() && !req.SerializeToString(&uncompressed_buffer)) {
            return HandleResult::RES_ERROR;
        }
        return result;
    }

    virtual HandleResult HandleRequest(uint64_t uin, const proto::ConsumerContext &cc, proto::Cookies &sys_cookies, T &req) = 0;
};

class HandlerFactory {
  public:
    HandlerFactory() {}
    virtual ~HandlerFactory() {}
    virtual Handler *New() = 0;
};

template <typename T>
    class DefaultHandlerFactory : public HandlerFactory {
  public:
    DefaultHandlerFactory() {}
    virtual ~DefaultHandlerFactory() {}

    Handler *New() {
        return new T();
    }
};


template <typename F>
    class FactoryList {
  public:
    FactoryList() {}
    ~FactoryList() {
        typename FList::iterator it;
        for (it = list_.begin(); list_.end() != it; ++it) {
            delete it->second, it->second = nullptr;
        }
    }

    void AddFactory(int handle_id, F *factory) {
        list_.push_back(std::make_pair(handle_id, factory));
    }

    F *GetFactory(int handle_id) {
        typename FList::iterator it;
        for (it = list_.begin(); list_.end() != it; ++it) {
            if (it->first == handle_id) return it->second;
        }
        return nullptr;
    }

  private:
    typedef std::vector<std::pair<int, F *>> FList;

    FList list_;
};


}  // namespace comm

}  // namespace phxqueue

