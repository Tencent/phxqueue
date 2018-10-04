#include "phxqueue/consumer/txqueryhandler.h"

#include "phxqueue/config.h"


namespace phxqueue {

namespace consumer {

using namespace std;

comm::HandleResult TxQueryHandler :: Handle(const comm::proto::ConsumerContext &cc, comm::proto::QItem &item, std::string &uncompressed_buffer)
{
    comm::RetCode ret;
    comm::proto::StatusInfo status_info;

    ret = GetTxStatus(cc.topic_id(), item.pub_id(), item.meta().client_id(), status_info);
    if (ret != comm::RetCode::RET_OK) {
        QLErr("GetTxStatus ret %d pub_id %d client_id %s", comm::as_integer(ret), item.pub_id(), item.meta().client_id().c_str());
    }

    if (status_info.tx_status() == comm::proto::TxStatus::TX_UNCERTAIN) {
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

        if (comm::RetCode::RET_OK != (ret = SelectSubscriberAddr(*item.mutable_sys_cookies(), cc.topic_id(), tx_query_sub_id, item.meta().uin(), *req.mutable_addr()))) {
            QLErr("SelectSubscriberAddr ret %d sub_id %d", comm::as_integer(ret), tx_query_sub_id);
        }

        if (comm::RetCode::RET_OK == ret && comm::RetCode::RET_OK != (ret = CallSubscriber(req, resp))) {
            QLErr("CallSubscriber ret %d", comm::as_integer(ret));
        } else {
            status_info.CopyFrom(resp.status_info());
        }
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

comm::RetCode TxQueryHandler::GetTxStatus(const int topic_id, const int pub_id, const std::string& client_id, comm::proto::StatusInfo &status_info)
{
    uint32_t version = 0;
    comm::RetCode ret = GetStatusInfo(topic_id, pub_id, client_id, status_info, version);
    return ret;
}

}
}
