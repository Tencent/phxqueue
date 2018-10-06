/*
Tencent is pleased to support the open source community by making PhxQueue available.
Copyright (C) 2017 THL A29 Limited, a Tencent company. All rights reserved.
Licensed under the BSD 3-Clause License (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at

<https://opensource.org/licenses/BSD-3-Clause>

Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.
*/



#pragma once

#include <functional>
#include <memory>
#include <set>
#include <vector>

#include "phxqueue/comm.h"

#include "phxqueue/producer/selector.h"
#include "phxqueue/producer/produceroption.h"
#include "phxqueue/txstatus.h"


namespace phxqueue {

namespace producer {


using ItemUpdateFunc = std::function<void (comm::proto::QItem &)>;

class Producer {
  public:
    Producer(const ProducerOption &opt);
    virtual ~Producer();

    // ------------------------ Interfaces generally used in main ------------------------
    // Usage please refer to phxqueue_phxrpc/test/producer_benchmark.cpp

    // Init.
    comm::RetCode Init();

    // Interface for Data first enqueue.
    // Pack argument info item, add to Store by Store::Add.
    comm::RetCode Enqueue(const uint64_t uin, const int topic_id, const int pub_id, const std::string &buffer, const std::string client_id = "");

    comm::RetCode Enqueue(const int topic_id, const uint64_t uin, const int handle_id, const std::string &buffer,
                          int pub_id = -1, const std::set<int> *consumer_group_ids = nullptr, const std::set<int> *sub_ids = nullptr, 
						  const std::string client_id = "");

    // ------------------------ Interfaces used in Reenqueue scene  ------------------------

    // Pack items info multiple AddRequests.
    // Items that belongs to the same queue_info is grouped together to facilitate batch adding to Store.
    // Parameters within Item can be modified by item_update_func.
    static comm::RetCode MakeAddRequests(const int topic_id,
                                         const std::vector<std::shared_ptr<comm::proto::QItem>> &items,
                                         std::vector<std::unique_ptr<comm::proto::AddRequest>> &reqs,
                                         ItemUpdateFunc item_update_func = nullptr);

    // Process a batch add to Store.
    // Customize StoreSelector/QueueSelector can be specified to determine which store/queue to add.
    comm::RetCode SelectAndAdd(comm::proto::AddRequest &req, comm::proto::AddResponse &resp,
                               StoreSelector *ss, QueueSelector *qs);

    // Process a batch add to Store.
    comm::RetCode RawAdd(comm::proto::AddRequest &req, comm::proto::AddResponse &resp);


    // ------------------------ Interfaces MUST be overrided ------------------------

    // Need to implement an RPC that corresponds to Store::Add().
    virtual comm::RetCode Add(const comm::proto::AddRequest &req,
                              comm::proto::AddResponse &resp) = 0;


    // ------------------------ Interfaces CAN be overrided ------------------------

  protected:

    // Return ProducerOption.
    const ProducerOption * GetProducerOption() const;

    // Implement customize StoreSelector to determine which store to add.
    virtual std::unique_ptr<QueueSelector> NewQueueSelector(const int topic_id, const int pub_id,
                                                            const uint64_t uin, const int count = 0,
                                                            const bool retry_switch_queue = false);

    // Implement customize QueueSelector to determine which queue to add.
    virtual std::unique_ptr<StoreSelector> NewStoreSelector(const int topic_id, const int pub_id,
                                                            const uint64_t uin,
                                                            const bool retry_switch_store = false);

    // Implement of set usercookies.
    virtual void SetUserCookies(comm::proto::Cookies &user_cookie) {}

    // Implement of set syscookies.
    virtual void SetSysCookies(comm::proto::Cookies &sys_cookie) {}

    // Implement of set syscookies.
    virtual void CompressBuffer(const std::string &buffer, std::string &compressed_buffer,
                                int &buffer_type) = 0;

    // If argument pub_id of Producer::Enqueue use default value, Producer need this implement to determine the real pub_id.
    virtual void DecidePubIDOnEnqueue(const int topic_id, const uint64_t uin,
                                      const int handle_id, int &pub_id) {}

    // Callback before Add.
    virtual void BeforeAdd(const comm::proto::AddRequest &req) {}

    // Callback after Add.
    virtual void AfterAdd(const comm::proto::AddRequest &req,
                          const comm::proto::AddResponse &resp) {}

  private:
    class ProducerImpl;
    std::unique_ptr<ProducerImpl> impl_;

    friend class BatchHelper;
};

class EventProducer : virtual public Producer, virtual public txstatus::TxStatusWriter 
{
public:
    EventProducer(const ProducerOption &opt) : Producer(opt), txstatus::TxStatusWriter() {}
    virtual ~EventProducer() {}

    comm::RetCode Prepare(const uint64_t uin, const int topic_id, const int pub_id, const std::string &buffer, const std::string client_id = "");
    comm::RetCode Commit(const int topic_id, const int pub_id, const std::string& client_id);
    comm::RetCode RollBack(const int topic_id, const int pub_id, const std::string& client_id);

protected:
    bool IsClientIDExist(const int topic_id, const int pub_id, const std::string& client_id);
    comm::RetCode CreateTxStatus(const int topic_id, const int pub_id, const std::string& client_id);

};


}  // namespace producer

}  // namespace phxqueue

