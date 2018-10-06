#pragma once
#include "phxqueue/comm.h"
#include "phxqueue/config.h"
#include "phxqueue/consumer.h"

#include "phxrpc/http.h"
#include "phxrpc/http/http_client.h"

namespace phxqueue_phxrpc {

namespace consumer {


template <typename Req, typename Resp>
class SubscriberCaller : virtual public phxqueue::consumer::SubscriberCaller<Req, Resp> {
public:
    SubscriberCaller() : phxqueue::consumer::SubscriberCaller<Req, Resp>() {}
    virtual ~SubscriberCaller() override {}
    virtual phxqueue::comm::RetCode CallSubscriber(const phxqueue::comm::proto::QItem &item, const int sub_id, const Req &req, Resp &resp, phxqueue::config::proto::RouteGeneral &route_general) override {
        phxrpc::BlockTcpStream socket;
        bool open_ret = phxrpc::BlockTcpUtils::Open(&socket, req.addr().ip().c_str(), req.addr().port(), route_general.conn_timeout_ms(), nullptr, 0);
        if (!open_ret) {
            QLErr("CallTxQuerySubscriber socket open %s:%d fail. pub %d client_id %s.",
                  req.addr().ip().c_str(), req.addr().port(), req.pub_id(), req.client_id().c_str());
            return phxqueue::comm::RetCode::RET_ERR_SYS;
        }

        phxrpc::HttpRequest http_req;
        phxrpc::HttpResponse http_resp;
        if (0 != http_req.FromPb(req)) {
            QLErr("CallTxQuerySubscriber parse request fail. pub %d client_id %s.", req.pub_id(), req.client_id().c_str());
            return phxqueue::comm::RetCode::RET_ERR_ARG;
        }
        http_req.set_uri(route_general.uri().c_str());

        int ret = phxrpc::HttpClient::Post(socket, http_req, &http_resp);
        if (0 == ret) {
            if (0 != http_resp.ToPb(&resp)) {
                QLErr("CallTxQuerySubscriber parse response fail. pub %d client_id %s.", req.pub_id(), req.client_id().c_str());
                return phxqueue::comm::RetCode::RET_ERR_LOGIC;
            }
            return phxqueue::comm::RetCode::RET_OK;
        }
        else {
            QLErr("CallTxQuerySubscriber post %s:%d fail, pub %d client_id %s ret %d statuscode %d reason %s",
                  req.addr().ip().c_str(), req.addr().port(), req.pub_id(), req.client_id().c_str(),
                  http_resp.status_code(), http_resp.reason_phrase());
            return phxqueue::comm::RetCode::RET_ERR_SYS;
        }
    }
};

}
}
