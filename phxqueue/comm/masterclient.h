/*
Tencent is pleased to support the open source community by making PhxQueue available.
Copyright (C) 2017 THL A29 Limited, a Tencent company. All rights reserved.
Licensed under the BSD 3-Clause License (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at

<https://opensource.org/licenses/BSD-3-Clause>

Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.
*/



#pragma once

#include <map>
#include <memory>
#include <random>
#include <sstream>

#include "phxqueue/comm/errdef.h"
#include "phxqueue/comm/logger.h"
#include "phxqueue/comm/proto/comm.pb.h"
#include "phxqueue/comm/utils.h"


namespace phxqueue {

namespace comm {


template <typename T> std::string GetRouteKey(T t) {
    std::ostringstream oss;
    oss << t;
    return oss.str();
}

template<typename T, typename... Args> std::string GetRouteKey(T t, Args... args) {
    std::ostringstream oss;
    oss << t << ":" << GetRouteKey(args...);
    return oss.str();
}

class MasterClientBase {
  public:
    MasterClientBase();
    virtual ~MasterClientBase();

  protected:
    void PutAddrToCache(const std::string &key, const proto::Addr &addr);
    bool GetAddrFromCache(const std::string &key, proto::Addr &addr);
    void RemoveCache(const std::string &key);

  private:
    thread_local static std::map<std::string, std::pair<proto::Addr, uint64_t> > addr_cache_;
};

template <typename Req, typename Resp>
class MasterClient : public MasterClientBase {
  public:
    MasterClient() {}
    virtual ~MasterClient() override {}

    RetCode ClientCall(Req &req, Resp &resp,
                       std::function<RetCode (Req &, Resp &)> rpc_func) {
        RetCode ret = RetCode::RET_OK;

        auto &&route_key = GetRouteKeyByReq(req);

        proto::Addr master_addr;
        if (!GetAddrFromCache(route_key, master_addr)) {
            // no master cache, find one
            QLVerb("no master cache. route_key(%s)", route_key.c_str());
        } else {
            req.mutable_master_addr()->CopyFrom(master_addr);
            ret = rpc_func(req, resp);

            QLVerb("rpc_func ret %d addr(%s:%d) route_key(%s)", as_integer(ret), master_addr.ip().c_str(),
                    master_addr.port(), route_key.c_str());

            if (RetCode::RET_OK == ret) {
                PutAddrToCache(route_key, master_addr);
                return RetCode::RET_OK;
            } else if (RetCode::RET_ERR_NOT_MASTER == ret &&
                        !resp.redirect_addr().ip().empty() &&
                        0 != resp.redirect_addr().port()) {
                // master change, update cache
                PutAddrToCache(route_key, resp.redirect_addr());
            } else if (RetCode::RET_ERR_NOT_MASTER == ret || RetCode::RET_ERR_NO_MASTER == ret ||
                        RetCode::RET_ERR_SVR_BLOCK == ret) {
                // no master, remove cache and find one
                RemoveCache(route_key);
            } else if (RetCode::RET_ERR_SYS == ret) {
                // sys err, not remove cache
            } else {
                QLErr("ClientCallByAddr ret %d addr(%s:%d) route_key(%s)", ret,
                      master_addr.ip().c_str(), master_addr.port(), route_key.c_str());
                return ret;
            }
        }

        std::vector<proto::Addr> addrs;
        {
            RetCode ret1;
            if (RetCode::RET_OK != (ret1 = GetCandidateAddrs(req, addrs))) {
                QLErr("GetCandidateAddrs err %d route_key(%s)", ret1, route_key.c_str());
                return ret1;
            }
        }
        if (!addrs.size()) {
            QLErr("GetCandidateAddrs addr size 0");
            return RetCode::RET_ERR_LOGIC;
        }

        int addrs_idx = 0;
        proto::Addr addr;
        if (RetCode::RET_ERR_NOT_MASTER == ret && GetAddrFromCache(route_key, addr)) {
            for (int i{0}; i < addrs.size(); ++i) {
                if (addr.ip() == addrs[i].ip() && addr.port() == addrs[i].port()) {
                    addrs_idx = i;
                    break;
                }
            }
        } else {
            addrs_idx = utils::OtherUtils::FastRand() % addrs.size();
            addr = addrs[addrs_idx];
            if (addr.ip() == master_addr.ip() && addr.port() == master_addr.port()) {
                addrs_idx = (addrs_idx + 1) % addrs.size();
                addr = addrs[addrs_idx];
            }
        }

        for (int i{0}; i < addrs.size(); ++i) {
            req.mutable_master_addr()->CopyFrom(addr);
            ret = rpc_func(req, resp);

            QLVerb("rpc_func again ret %d addr(%s:%d) route_key(%s)", as_integer(ret), addr.ip().c_str(),
                    addr.port(), route_key.c_str());

            if (RetCode::RET_OK == ret) {
                PutAddrToCache(route_key, addr);
                return RetCode::RET_OK;
            } else if (RetCode::RET_ERR_NOT_MASTER == ret &&
                        !resp.redirect_addr().ip().empty() &&
                        0 != resp.redirect_addr().port()) {
                // master change, update cache
                addr.CopyFrom(resp.redirect_addr());
                PutAddrToCache(route_key, addr);
            } else if (RetCode::RET_ERR_NOT_MASTER == ret || RetCode::RET_ERR_NO_MASTER == ret ||
                        RetCode::RET_ERR_SVR_BLOCK == ret ||
                        RetCode::RET_ERR_SYS == ret) {
                addrs_idx = (addrs_idx + 1) % addrs.size();
                addr = addrs[addrs_idx];
            } else {
                break;
            }
        }

        return ret;
    }

  protected:
    virtual std::string GetRouteKeyByReq(const Req &req) = 0;
    virtual RetCode GetCandidateAddrs(const Req &req, std::vector<proto::Addr> &addrs) = 0;
};


}  // namespace comm

}  // namespace phxqueue

