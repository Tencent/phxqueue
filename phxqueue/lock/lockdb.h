/*
Tencent is pleased to support the open source community by making PhxQueue available.
Copyright (C) 2017 THL A29 Limited, a Tencent company. All rights reserved.
Licensed under the BSD 3-Clause License (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at

<https://opensource.org/licenses/BSD-3-Clause>

Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.
*/



#pragma once

#include <map>

#include "leveldb/db.h"

#include "phxqueue/lock/lock.h"
#include "phxqueue/lock/proto/lock.pb.h"


namespace phxqueue {

namespace lock {


// TODO: Change to template
class LockDb {
  public:
    enum class StorageType {NONE, MAP, LEVELDB, MAX};
    enum class DataType {NONE, IGNORE, USER, MAX};

    LockDb();

    // noncopyable
    LockDb(const LockDb &) = delete;

    LockDb(LockDb &&rhs);

    virtual ~LockDb();

    // noncopyable
    LockDb &operator=(const LockDb &) = delete;

    LockDb &operator=(LockDb &&rhs);

    comm::RetCode Init(const StorageType storage_type, const std::string &leveldb_file);
    comm::RetCode Dispose();

    comm::RetCode VersionSetString(const proto::RecordKeyInfo &record_key_info,
                                   const proto::LocalRecordInfo &v, const bool sync = true);
    comm::RetCode VersionDeleteString(const proto::RecordKeyInfo &record_key_info,
                                      const bool sync = true);

    comm::RetCode AcquireLock(const proto::RecordKeyInfo &record_key_info,
                              const proto::LocalRecordInfo &v, const bool sync = true);
    comm::RetCode AcquireLock(const proto::RecordKeyInfo &record_key_info,
                              const std::string &vstr, const bool sync = true);

//    comm::RetCode ReleaseLock(const phxqueue_proto::LockKeyInfo &lock_key_info, const bool sync = true);

//    comm::RetCode CleanLock(const google::protobuf::RepeatedPtrField<phxqueue_proto::LockKeyInfo> &
//                      lock_key_infos, const bool sync = true);

    comm::RetCode GetString(const std::string &k, proto::LocalRecordInfo &v) const;
    comm::RetCode SetString(const std::string &k, const proto::LocalRecordInfo &v, const bool sync);
    comm::RetCode DeleteString(const std::string &k, const bool sync);

    comm::RetCode GetLock(const std::string &k, proto::LocalRecordInfo &v) const;
    comm::RetCode SetLock(const std::string &k, const proto::LocalRecordInfo &v, const bool sync);
    comm::RetCode DeleteLock(const std::string &k, const bool sync);

    inline pthread_mutex_t *const mutex() { return mutex_; }

  private:
    friend class LockMgr;
    friend class CleanThread;

    comm::RetCode GetRecord(const std::string &k, proto::LocalRecordInfo &v) const;
    comm::RetCode SetRecord(const std::string &k, const proto::LocalRecordInfo &v, const bool sync = true);
    comm::RetCode DeleteRecord(const std::string &k, const bool sync = true);

    comm::RetCode DiskGet(const std::string &k, std::string &vstr) const;
    comm::RetCode DiskSet(const std::string &k, const std::string &vstr, const bool sync = true);
    comm::RetCode DiskDelete(const std::string &k, const bool sync = true);

    // write something to trigger sync
    comm::RetCode Sync();

    void SeekToFirstRecord();
    void NextRecord();
    bool ValidRecord() const;
    comm::RetCode GetCurrentRecord(std::string *const k, std::string *const vstr,
                                   proto::LocalRecordInfo *const local_record_info);

    void DiskSeekToFirst();
    void DiskNext();
    bool DiskValid() const;
    comm::RetCode DiskGetCurrent(std::string *const k, std::string *const vstr,
                                 DataType *const data_type);

    inline int GetSizeRecord() { return StorageType::MAP == storage_type_ ? map_.size() : 0; }
    comm::RetCode ForwardLoopKey(std::string *const k);

    pthread_mutex_t *mutex_{nullptr};
    StorageType storage_type_{StorageType::NONE};
    std::map<std::string, proto::LocalRecordInfo> map_;
    std::map<std::string, proto::LocalRecordInfo>::const_iterator map_it_;
    leveldb::DB *leveldb_{nullptr};
    leveldb::Iterator *leveldb_it_{nullptr};
    bool leveldb_checks_{true};
    std::string prev_loop_key_;
    // must modify move constructor & move assignment at the same time
};


}  // namespace lock

}  // namespace phxqueue

