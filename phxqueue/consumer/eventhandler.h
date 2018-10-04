#pragma once
#include "phxqueue/comm.h"
#include "phxqueue/txstatus.h"

namespace phxqueue {

namespace consumer {

class TxQueryHandler : public comm::Handler, public txstatus::TxStatusReader {
	public:
		TxQueryHandler();
		virtual ~TxQueryHandler();

		virtual comm::HandleResult Handle(const comm::proto::ConsumerContext &cc, comm::proto::QItem &item, std::string &uncompressed_buffer);

		virtual void CheckTxStatus(const comm::proto::ConsumerContext &cc, comm::proto::QItem &item, comm::proto::StatusInfo &status_info);

		virtual void SelectTxQuerySubscriberAddr(comm::proto::Cookies &sys_cookies, const int sub_id, const uint64_t uin, 
									   	   		 comm::proto::TxQueryRequest &req, comm::proto::TxQueryResponse &resp);

		virtual void CallTxQuerySubscriber(const comm::proto::TxQueryRequest &req, comm::proto::TxQueryResponse &resp) = 0;
};

class PushHandler : public comm::Handler {
	public:
		PushHandler();
		virtual ~PushHandler();

		virtual comm::HandleResult Handle(const comm::proto::ConsumerContext &cc, comm::proto::QItem &item, std::string &uncompressed_buffer);

		virtual void SelectSubscriberAddr(comm::proto::Cookies &sys_cookies, const int sub_id, const uint64_t uin, 
										  comm::proto::PushRequest &req, comm::proto::PushResponse &resp);

		virtual void CallSubscriber(const comm::proto::PushRequest &req, comm::proto::PushResponse &resp) = 0;
};

}
}

