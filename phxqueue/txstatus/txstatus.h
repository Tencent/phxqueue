/*
Tencent is pleased to support the open source community by making PhxQueue available.
Copyright (C) 2017 THL A29 Limited, a Tencent company. All rights reserved.
Licensed under the BSD 3-Clause License (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at

<https://opensource.org/licenses/BSD-3-Clause>

Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.
*/



#pragma once

#include "phxqueue/comm.h"

namespace phxqueue {

namespace txstatus {

using namespace std;

class TxStatusReader
{
	public:
		TxStatusReader();
		virtual ~TxStatusReader();

		bool IsClientIDExist(const int topic_id, const int pub_id, const std::string& client_id);

		comm::RetCode GetTxStatus(const int topic_id, const int pub_id, const std::string& client_id, comm::proto::StatusInfo &status_info);

	protected:
		void GenKey(const int topic_id, const int pub_id, const std::string& client_id, std::string &key, uint32_t &hashkey);

		comm::RetCode GetLockID(const int topic_id, const uint32_t hashkey, int &lock_id);

		comm::RetCode GetStatusInfo(const int topic_id, const int pub_id, const std::string& client_id, comm::proto::StatusInfo &status_info, uint32_t &version);

		virtual comm::RetCode GetStatusInfoFromLock(const comm::proto::GetStringRequest &req, comm::proto::GetStringResponse &resp) = 0;
};

class TxStatusWriter : public TxStatusReader
{
	public:
		TxStatusWriter();
		virtual ~TxStatusWriter();

		comm::RetCode CreateTxStatus(const int topic_id, const int pub_id, const std::string& client_id);

		comm::RetCode Commit(const int topic_id, const int pub_id, const std::string& client_id);

		comm::RetCode RollBack(const int topic_id, const int pub_id, const std::string& client_id);

	protected:
		comm::RetCode SetStatusInfo(const int topic_id, const int pub_id, const std::string& client_id, const comm::proto::StatusInfo &status_info, const uint32_t version);

		virtual comm::RetCode SetStatusInfoToLock(const comm::proto::SetStringRequest &req, comm::proto::SetStringResponse &resp) = 0;

	private:
		const uint64_t CLIENTID_EXPIRE_TIME = 3 * 24 * 60 * 60 * 1000;
};

}
}

