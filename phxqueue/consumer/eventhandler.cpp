#include "phxqueue/consumer/eventhandler.h"

#include "phxqueue/config.h"


namespace phxqueue {

namespace consumer {

using namespace std;


TxQueryHandler :: TxQueryHandler() {
}

TxQueryHandler :: ~TxQueryHandler() {
}

comm::HandleResult TxQueryHandler :: Handle(const comm::proto::ConsumerContext &cc, comm::proto::QItem &item, std::string &uncompressed_buffer)
{
	comm::proto::StatusInfo status_info;

	CheckTxStatus(cc, item, status_info);

	if (status_info.tx_status() == comm::proto::TxStatus::TX_UNCERTAIN) {
		phxqueue::comm::RetCode ret;
		shared_ptr<const config::TopicConfig> topic_config;
		if (comm::RetCode::RET_OK != (ret = config::GlobalConfig::GetThreadInstance()->GetTopicConfigByTopicID(cc.topic_id(), topic_config))) {
			QLErr("GetTopicConfigByTopicID ret %d topic_id %d", comm::as_integer(ret), cc.topic_id());
			return comm::HandleResult::RES_ERROR;
		}

		int tx_query_sub_id = 0;
		if (comm::RetCode::RET_OK != (ret = topic_config->GetTxQuerySubIDByPubID(item.pub_id(), tx_query_sub_id))) {
			QLErr("GetTxQuerySubIDByPubID ret %d pub_id %d client_id %s", comm::as_integer(ret), item.pub_id(), item.meta().client_id().c_str());
			return comm::HandleResult::RES_ERROR;
		}

		comm::proto::TxQueryRequest req;
		comm::proto::TxQueryResponse resp;

		req.set_topic_id(cc.topic_id());
		req.set_pub_id(item.pub_id());
		req.set_client_id(item.meta().client_id());
		req.set_count(item.count());
		req.set_atime(item.meta().atime());
		req.set_buffer(uncompressed_buffer);

		CallTxQuerySubscriber(*item.mutable_sys_cookies(), tx_query_sub_id, item.meta().uin(), req, resp);

		status_info.CopyFrom(resp.status_info());
	}

	comm::HandleResult res;
	if (status_info.tx_status() == comm::proto::TxStatus::TX_COMMIT) {
		if (status_info.has_commit_buffer()) {
			uncompressed_buffer = status_info.commit_buffer();
			SetBufferUpdated(true);
		}
		res = comm::HandleResult::RES_OK;
	}
	else if (status_info.tx_status() == comm::proto::TxStatus::TX_ROLLBACK) {
		res = comm::HandleResult::RES_IGNORE;
	}
	else {
		res = comm::HandleResult::RES_ERROR;
	}

	return res;
}


PushHandler :: PushHandler() {
}

PushHandler :: ~PushHandler() {
}


comm::HandleResult PushHandler :: Handle(const comm::proto::ConsumerContext &cc, comm::proto::QItem &item, std::string &uncompressed_buffer)
{
	phxqueue::comm::RetCode ret;
	shared_ptr<const config::TopicConfig> topic_config;
	if (comm::RetCode::RET_OK != (ret = config::GlobalConfig::GetThreadInstance()->GetTopicConfigByTopicID(cc.topic_id(), topic_config))) {
		QLErr("GetTopicConfigByTopicID ret %d topic_id %d", comm::as_integer(ret), cc.topic_id());
		return comm::HandleResult::RES_ERROR;
	}

	std::set<int> sub_ids;
	if (item.count() == 0) {
		for (int i{0}; i < item.sub_ids_size(); i++) {
			shared_ptr<const config::proto::Sub> sub;
			if (comm::RetCode::RET_OK != (ret = topic_config->GetSubBySubID(item.sub_ids(i), sub))) {
				QLErr("GetSubBySubID ret %d sub_id %d", comm::as_integer(ret), item.sub_ids(i));
				return comm::HandleResult::RES_ERROR;
			}
			if (sub->consumer_group_id() == cc.consumer_group_id()) {
				sub_ids.insert(item.sub_ids(i));
			}
		}
	}
	else {
		for (int i{0}; i < item.sub_ids_size(); i++) {
			sub_ids.insert(item.sub_ids(i));
		}
	}

	if (sub_ids.empty()) {
		return comm::HandleResult::RES_OK;
	}

	comm::proto::PushRequest req;
	req.set_topic_id(cc.topic_id());
	req.set_pub_id(item.pub_id());
	req.set_client_id(item.meta().client_id());
	req.set_count(item.count());
	req.set_atime(item.meta().atime());
	req.set_buffer(uncompressed_buffer);

	std::set<int> retry_sub_ids;
	int succ_count = 0;
	int fail_count = 0;
	for (auto &&sub_id : sub_ids) {
		comm::proto::PushResponse resp;
		CallSubscriber(*item.mutable_sys_cookies(), sub_id, item.meta().uin(), req, resp);

		if (resp.result() == "success") {
			++ succ_count;
		}
		else {
			++ fail_count;
			retry_sub_ids.insert(sub_id);
		}
	}

	NLInfo("pub_id %d clientid %s consumer_group_id %d succ %d fail %d", item.pub_id(), item.meta().client_id().c_str(), cc.consumer_group_id(), succ_count, fail_count);

	if (fail_count) {
		item.clear_sub_ids();
		for (auto sub_id : retry_sub_ids) {
			item.add_sub_ids(sub_id);
		}
	}

	comm::HandleResult res = (fail_count == 0)? comm::HandleResult::RES_OK : comm::HandleResult::RES_ERROR;

	return res;
}



}
}

//gzrd_Lib_CPP_Version_ID--start
#ifndef GZRD_SVN_ATTR
#define GZRD_SVN_ATTR "0"
#endif
static char gzrd_Lib_CPP_Version_ID[] __attribute__((used))="$HeadURL$ $Id$ " GZRD_SVN_ATTR "__file__";
// gzrd_Lib_CPP_Version_ID--end

