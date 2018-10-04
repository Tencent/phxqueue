#include "eventhandler.h"
#include "phxqueue_phxrpc/app/lock/lock_client.h"
#include "phxrpc/http/http_client.h"

namespace phxqueue_phxrpc {

namespace consumer {

TxQueryHandler :: TxQueryHandler()
{
}

TxQueryHandler :: ~TxQueryHandler()
{
}

phxqueue::comm::RetCode TxQueryHandler :: GetStatusInfoFromLock(const phxqueue::comm::proto::GetStringRequest &req, phxqueue::comm::proto::GetStringResponse &resp)
{
	LockClient lock_client;
	auto ret = lock_client.ProtoGetString(req, resp);
    if (phxqueue::comm::RetCode::RET_OK != ret) {
        QLErr("GetString ret %d", phxqueue::comm::as_integer(ret));
    }
    return ret;
}

void TxQueryHandler :: CallTxQuerySubscriber(const phxqueue::comm::proto::TxQueryRequest &req, phxqueue::comm::proto::TxQueryResponse &resp)
{
	phxrpc::BlockTcpStream socket;
	bool open_ret = phxrpc::BlockTcpUtils::Open(&socket, req.addr().ip().c_str(), req.addr().port(), 500, nullptr, 0);
	if (!open_ret) {
        QLErr("CallTxQuerySubscriber socket open %s:%d fail. pub %d client_id %s.", 
				req.addr().ip().c_str(), req.addr().port(), req.pub_id(), req.client_id().c_str());
		return ;
	}

	phxrpc::HttpRequest http_req;
	phxrpc::HttpResponse http_resp;
	if (0 != http_req.FromPb(req)) {
        QLErr("CallTxQuerySubscriber parse request fail. pub %d client_id %s.", req.pub_id(), req.client_id().c_str());
		return ;
	}

	int ret = phxrpc::HttpClient::Post(socket, http_req, &http_resp);
	if (ret) {
		if (0 != http_resp.ToPb(&resp)) {
			QLErr("CallTxQuerySubscriber parse response fail. pub %d client_id %s.", req.pub_id(), req.client_id().c_str());
			return ;
		}
	}
	else {
        QLErr("CallTxQuerySubscriber post %s:%d fail, pub %d client_id %s ret %d statuscode %d reason %s", 
				req.addr().ip().c_str(), req.addr().port(), req.pub_id(), req.client_id().c_str(),
				http_resp.status_code(), http_resp.reason_phrase());
	}
}


PushHandler :: PushHandler()
{
}

PushHandler :: ~PushHandler()
{
}

void PushHandler :: CallSubscriber(const phxqueue::comm::proto::PushRequest &req, phxqueue::comm::proto::PushResponse &resp)
{
	phxrpc::BlockTcpStream socket;
	bool open_ret = phxrpc::BlockTcpUtils::Open(&socket, req.addr().ip().c_str(), req.addr().port(), 500, nullptr, 0);
	if (!open_ret) {
        QLErr("CallTxQuerySubscriber socket open %s:%d fail. pub %d client_id %s.", 
				req.addr().ip().c_str(), req.addr().port(), req.pub_id(), req.client_id().c_str());
		return ;
	}

	phxrpc::HttpRequest http_req;
	phxrpc::HttpResponse http_resp;
	if (0 != http_req.FromPb(req)) {
        QLErr("CallTxQuerySubscriber parse request fail. pub %d client_id %s.", req.pub_id(), req.client_id().c_str());
		return ;
	}

	int ret = phxrpc::HttpClient::Post(socket, http_req, &http_resp);
	if (ret) {
		if (0 != http_resp.ToPb(&resp)) {
			QLErr("CallTxQuerySubscriber parse response fail. pub %d client_id %s.", req.pub_id(), req.client_id().c_str());
			return ;
		}
	}
	else {
        QLErr("CallTxQuerySubscriber post %s:%d fail, pub %d client_id %s ret %d statuscode %d reason %s", 
				req.addr().ip().c_str(), req.addr().port(), req.pub_id(), req.client_id().c_str(),
				http_resp.status_code(), http_resp.reason_phrase());
	}
}

}
}
