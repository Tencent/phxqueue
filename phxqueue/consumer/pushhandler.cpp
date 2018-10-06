#include "phxqueue/consumer/pushhandler.h"

#include "phxqueue/config.h"


namespace phxqueue {

namespace consumer {

using namespace std;


comm::HandleResult PushHandler :: Handle(const comm::proto::ConsumerContext &cc, comm::proto::QItem &item, std::string &uncompressed_buffer)
{
    phxqueue::comm::RetCode ret;
    shared_ptr<const config::TopicConfig> topic_config;
    if (comm::RetCode::RET_OK != (ret = config::GlobalConfig::GetThreadInstance()->GetTopicConfigByTopicID(cc.topic_id(), topic_config))) {
        QLErr("GetTopicConfigByTopicID ret %d topic_id %d", comm::as_integer(ret), cc.topic_id());
        return comm::HandleResult::RES_ERROR;
    }

    std::set<int> sub_ids;
    if (0 == item.sub_ids_size()){
        std::set<int> tmp_sub_ids;
        if (comm::RetCode::RET_OK != (ret = topic_config->GetSubIDsByPubID(item.pub_id(), tmp_sub_ids))) {
            QLErr("GetSubIDsByPubID ret %d pub_id %d", comm::as_integer(ret), item.pub_id());
            return comm::HandleResult::RES_ERROR;
        }

        for (auto &&sub_id : tmp_sub_ids) {
            shared_ptr<const config::proto::Sub> sub;
            if (comm::RetCode::RET_OK != (ret = topic_config->GetSubBySubID(sub_id, sub))) {
                QLErr("GetSubBySubID ret %d sub_id %d", comm::as_integer(ret), sub_id);
                return comm::HandleResult::RES_ERROR;
            }
            if (sub->consumer_group_id() == cc.consumer_group_id()) {
                sub_ids.insert(sub_id);
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
        req.mutable_addr()->Clear();

        config::proto::RouteGeneral route_general;
        if (comm::RetCode::RET_OK != (ret = SelectSubscriberAddr(item, sub_id, *req.mutable_addr(), route_general))) {
            QLErr("SelectSubscriberAddr ret %d sub_id %d", comm::as_integer(ret), sub_id);
        }

        if (comm::RetCode::RET_OK == ret && comm::RetCode::RET_OK != (ret = CallSubscriber(item, sub_id, req, resp, route_general))) {
            QLErr("CallSubscriber ret %d", comm::as_integer(ret));
        }

        if (resp.result() == "success") {
            ++succ_count;
        }
        else {
            ++fail_count;
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
