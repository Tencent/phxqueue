#pragma once
#include "phxqueue/consumer/eventhandler.h"

namespace phxqueue_phxrpc {

namespace consumer {


class TxQueryHandler : public phxqueue::consumer::TxQueryHandler {
	public:
		TxQueryHandler();
		virtual ~TxQueryHandler();

		virtual void CallTxQuerySubscriber(const phxqueue::comm::proto::TxQueryRequest &req, phxqueue::comm::proto::TxQueryResponse &resp);

		virtual phxqueue::comm::RetCode GetStatusInfoFromLock(const phxqueue::comm::proto::GetStringRequest &req, phxqueue::comm::proto::GetStringResponse &resp);
};



class PushHandler : public phxqueue::consumer::PushHandler {
	public:
		PushHandler();
		virtual ~PushHandler();

		virtual void CallSubscriber(const phxqueue::comm::proto::PushRequest &req, phxqueue::comm::proto::PushResponse &resp) = 0;
};


}
}
