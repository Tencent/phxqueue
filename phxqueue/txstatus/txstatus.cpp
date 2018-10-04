#include "phxqueue/config.h"
#include "phxqueue/lock.h"
#include "phxqueue/comm.h"
#include "phxqueue/txstatus/txstatus.h"

namespace phxqueue {
namespace txstatus {

/***   TxStatusReader      ***/

TxStatusReader :: TxStatusReader()
{
}

TxStatusReader :: ~TxStatusReader()
{
}

void TxStatusReader :: GenKey(int topic_id, const int pub_id, const std::string& client_id, std::string &key, uint32_t &hashkey)
{
	char pcKey[300];
	snprintf(pcKey, sizeof(pcKey), "txst_%d#%d#%s", topic_id, pub_id, client_id.c_str());
	key = std::string(pcKey);

	uint32_t seed = 131;
	hashkey = 0;
	for (int i = 0; i < key.size(); i++) {
		hashkey = hashkey * seed + key[i];
	}
}

comm::RetCode TxStatusReader :: GetLockID(const int topic_id, const uint32_t hashkey, int &lock_id)
{
    comm::RetCode ret;
    shared_ptr<const config::LockConfig> lock_config;
    if (comm::RetCode::RET_OK != (ret = config::GlobalConfig::GetThreadInstance()->GetLockConfig(topic_id, lock_config))) {
        QLErr("ERR: GetLockConfig ret %d topic_id %u", comm::as_integer(ret), topic_id);
        return ret;
    }

    set<int> lock_ids;
    if (comm::RetCode::RET_OK != (ret = lock_config->GetAllLockID(lock_ids))) {
        QLErr("ERR: GetAllLockID ret %d", comm::as_integer(ret));
        return ret;
    }

    if (lock_ids.empty()) {
        QLErr("ERR: lock_ids empty");
        return comm::RetCode::RET_ERR_RANGE_LOCK;
    }

	int idx = hashkey % lock_ids.size();

	auto iter = lock_ids.begin();
	for (int i = 0; i < idx; i++) {
		iter ++;
	}
	lock_id = *iter;

	return comm::RetCode::RET_OK;
}

comm::RetCode TxStatusReader :: GetStatusInfo(const int topic_id, const int pub_id, const std::string& client_id, comm::proto::StatusInfo &status_info, uint32_t &version)
{
    comm::RetCode ret;

	std::string key;
	uint32_t hashkey;
	GenKey(topic_id, pub_id, client_id, key, hashkey);

	int lock_id = 0;
	if (comm::RetCode::RET_OK != (ret = GetLockID(topic_id, hashkey, lock_id))) {
		return ret;
	}

	comm::proto::GetStringRequest req;
	comm::proto::GetStringResponse resp;
	req.set_topic_id(topic_id);
	req.set_lock_id(lock_id);
	req.set_key(key);

	lock::LockMasterClient<comm::proto::GetStringRequest, comm::proto::GetStringResponse> lock_master_client;
	ret = lock_master_client.ClientCall(req, resp, bind(&TxStatusReader::GetStatusInfoFromLock, this, placeholders::_1, placeholders::_2));
	if (comm::RetCode::RET_OK != ret) {
		QLErr("ERR: topicid %d pubid %d clientid %s GetStatusInfoFromLock ret %d", 
				topic_id, pub_id, client_id.c_str(), comm::as_integer(ret));
	}
	else {
		if (!status_info.ParseFromString(resp.string_info().value())) {
			QLErr("ERR: topicid %d pubid %d clientid %s GetStatusInfoFromLock status_info parse fail",
					topic_id, pub_id, client_id.c_str());
			return comm::RetCode::RET_ERR_PROTOBUF_PARSE;
		}
		version = resp.string_info().version();
	}

	return ret;
}

/***   TxStatusWriter      ***/

TxStatusWriter :: TxStatusWriter()
{
}

TxStatusWriter :: ~TxStatusWriter()
{
}



comm::RetCode TxStatusWriter :: SetStatusInfo(const int topic_id, const int pub_id, const std::string& client_id, const comm::proto::StatusInfo &status_info, const uint32_t version)
{
	comm::RetCode ret;

	std::string key;
	uint32_t hashkey;
	GenKey(topic_id, pub_id, client_id, key, hashkey);

	int lock_id = 0;
	if (comm::RetCode::RET_OK != (ret = GetLockID(topic_id, hashkey, lock_id))) {
		return ret;
	}

	std::string value;
	status_info.SerializeToString(&value);

	comm::proto::SetStringRequest req;
	comm::proto::SetStringResponse resp;
	req.set_topic_id(topic_id);
	req.set_lock_id(lock_id);
	req.mutable_string_info()->set_key(key);
	req.mutable_string_info()->set_value(value);
	req.mutable_string_info()->set_version(version);
	req.mutable_string_info()->set_lease_time_ms(CLIENTID_EXPIRE_TIME);

	lock::LockMasterClient<comm::proto::SetStringRequest, comm::proto::SetStringResponse> lock_master_client;
	ret = lock_master_client.ClientCall(req, resp, bind(&TxStatusWriter::SetStatusInfoToLock, this, placeholders::_1, placeholders::_2));
	if (comm::RetCode::RET_OK != ret) {
		QLErr("ERR: topicid %d pubid %d clientid %s SetStatusInfoToLock ret %d", 
				topic_id, pub_id, client_id.c_str(), comm::as_integer(ret));
	}

	return ret;
}



}
}

//gzrd_Lib_CPP_Version_ID--start
#ifndef GZRD_SVN_ATTR
#define GZRD_SVN_ATTR "0"
#endif
static char gzrd_Lib_CPP_Version_ID[] __attribute__((used))="$HeadURL$ $Id$ " GZRD_SVN_ATTR "__file__";
// gzrd_Lib_CPP_Version_ID--end

