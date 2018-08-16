/*
Tencent is pleased to support the open source community by making PhxQueue available.
Copyright (C) 2017 THL A29 Limited, a Tencent company. All rights reserved.
Licensed under the BSD 3-Clause License (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at

<https://opensource.org/licenses/BSD-3-Clause>

Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.
*/



#include "phxqueue/lock/lockdb.h"

#include "leveldb/write_batch.h"

#include "phxqueue/comm.h"


namespace {


// for system only, user cannot use key with this prefix
constexpr char *KEY_IGNORE_PREFIX{"__ignore__"};
// meaningless, only for trigger sync
constexpr char *KEY_IGNORE_SYNC_TRIGGER{"__ignore__:__sync_trigger__"};
// for user
constexpr char *KEY_USER_STRING_PREFIX{""};
constexpr char *KEY_USER_LOCK_PREFIX{""};


}  // namespace


namespace phxqueue {

namespace lock {


using namespace std;


LockDb::LockDb() {
    mutex_ = new pthread_mutex_t;
    pthread_mutex_init(mutex_, nullptr);
    QLVerb("init mutex");
}

LockDb::LockDb(LockDb &&rhs) {
    mutex_ = rhs.mutex_;
    rhs.mutex_ = nullptr;
    storage_type_ = rhs.storage_type_;
    map_ = move(rhs.map_);
    map_it_ = move(rhs.map_it_);
    leveldb_ = rhs.leveldb_;
    rhs.leveldb_ = nullptr;
    leveldb_it_ = rhs.leveldb_it_;
    rhs.leveldb_it_ = nullptr;
    leveldb_checks_ = rhs.leveldb_checks_;
    prev_loop_key_ = move(rhs.prev_loop_key_);
}

LockDb::~LockDb() {
    if (nullptr != leveldb_it_) {
        delete leveldb_it_;
        leveldb_it_ = nullptr;
    }

    if (nullptr != leveldb_) {
        delete leveldb_;
        leveldb_ = nullptr;
    }

    if (nullptr != mutex_) {
        QLVerb("destroy mutex");
        pthread_mutex_destroy(mutex_);
        delete mutex_;
        mutex_ = nullptr;
    }
}

LockDb &LockDb::operator=(LockDb &&rhs) {
    mutex_ = rhs.mutex_;
    rhs.mutex_ = nullptr;
    storage_type_ = rhs.storage_type_;
    map_ = move(rhs.map_);
    map_it_ = move(rhs.map_it_);
    leveldb_ = rhs.leveldb_;
    rhs.leveldb_ = nullptr;
    leveldb_it_ = rhs.leveldb_it_;
    rhs.leveldb_it_ = nullptr;
    leveldb_checks_ = rhs.leveldb_checks_;
    prev_loop_key_ = move(rhs.prev_loop_key_);

    return *this;
}

comm::RetCode LockDb::Init(const LockDb::StorageType type, const string &leveldb_file) {
    storage_type_ = type;

    if (StorageType::MAP == storage_type_) {
    } else if (StorageType::LEVELDB == storage_type_) {
        leveldb::Options options;
        options.create_if_missing = true;
        // raise an error as soon as it detects an internal corruption
        options.paranoid_checks = leveldb_checks_;
        leveldb::Status status(leveldb::DB::Open(options, leveldb_file, &leveldb_));
        if (!status.ok()) {
            QLErr("open \"%s\" err \"%s\"", leveldb_file.c_str(), status.ToString().c_str());

            return comm::RetCode::RET_ERR_LEVELDB;
        }
    } else {
        return comm::RetCode::RET_ERR_NO_IMPL;
    }

    return comm::RetCode::RET_OK;
}

comm::RetCode LockDb::Dispose() {
    if (StorageType::MAP == storage_type_) {
        map_.clear();
    } else if (StorageType::LEVELDB == storage_type_) {
        if (nullptr != leveldb_) {
            delete leveldb_;
            leveldb_ = nullptr;
        }
    } else {
        return comm::RetCode::RET_ERR_NO_IMPL;
    }

    return comm::RetCode::RET_OK;
}

comm::RetCode LockDb::VersionSetString(const proto::RecordKeyInfo &record_key_info,
                                       const proto::LocalRecordInfo &v, const bool sync) {
    comm::LockDbBP::GetThreadInstance()->OnVersionSetString(sync);
    proto::LocalRecordInfo localv;
    comm::RetCode ret{GetString(record_key_info.key(), localv)};
    if (comm::RetCode::RET_ERR_KEY_NOT_EXIST == ret) {
        QLVerb("storage %d key \"%s\" not exist", static_cast<int>(storage_type_),
               record_key_info.key().c_str());
    } else if (comm::RetCode::RET_OK != ret) {
        QLErr("storage %d key \"%s\" err %d", static_cast<int>(storage_type_),
              record_key_info.key().c_str(), ret);

        return ret;
    }

    if (comm::RetCode::RET_ERR_KEY_NOT_EXIST != ret && UINT64_MAX != record_key_info.version() &&
        localv.version() != record_key_info.version()) {
        QLErr("storage %d key \"%s\" local.ver %llu != req.ver %llu err",
              static_cast<int>(storage_type_), record_key_info.key().c_str(),
              localv.version(), record_key_info.version());

        return comm::RetCode::RET_ERR_VERSION_NOT_EQUAL;
    }

    if (0 == v.lease_time_ms()) {
        QLVerb("storage %d key \"%s\" lease_time_ms %llu delete",
               static_cast<int>(storage_type_), record_key_info.key().c_str(), v.lease_time_ms());
        return DeleteString(record_key_info.key(), sync);
    }

    // TODO: not copy?
    proto::LocalRecordInfo v2;
    v2.set_version(v.version());
    v2.set_value(v.value());
    v2.set_lease_time_ms(v.lease_time_ms());
    if (-1 != v2.lease_time_ms()) {
        v2.set_expire_time_ms(comm::utils::Time::GetSteadyClockMS() + v.lease_time_ms());
    }
    QLVerb("storage %d key \"%s\" ver %llu expire_time_ms %llu", static_cast<int>(storage_type_),
           record_key_info.key().c_str(), v2.version(), v2.expire_time_ms());

    return SetString(record_key_info.key(), v2, sync);
}

comm::RetCode LockDb::VersionDeleteString(const proto::RecordKeyInfo &record_key_info,
                                          const bool sync) {
    comm::LockDbBP::GetThreadInstance()->OnVersionDeleteString(sync);
    proto::LocalRecordInfo localv;
    comm::RetCode ret{GetString(record_key_info.key(), localv)};
    if (comm::RetCode::RET_ERR_KEY_NOT_EXIST == ret) {
        QLVerb("storage %d key \"%s\" not exist", static_cast<int>(storage_type_),
               record_key_info.key().c_str());

        return comm::RetCode::RET_OK;
    } else if (comm::RetCode::RET_OK != ret) {
        QLErr("storage %d key \"%s\" err %d", static_cast<int>(storage_type_),
              record_key_info.key().c_str(), ret);

        return ret;
    }

    if (comm::RetCode::RET_ERR_KEY_NOT_EXIST != ret && UINT64_MAX != record_key_info.version() &&
        localv.version() != record_key_info.version()) {
        QLErr("storage %d key \"%s\" local.ver %llu != req.ver %llu err", static_cast<int>(storage_type_),
              record_key_info.key().c_str(), localv.version(), record_key_info.version());

        return comm::RetCode::RET_ERR_VERSION_NOT_EQUAL;
    }

    return DeleteString(record_key_info.key(), sync);
}

comm::RetCode LockDb::AcquireLock(const proto::RecordKeyInfo &record_key_info,
                                  const proto::LocalRecordInfo &v, const bool sync) {
    comm::LockDbBP::GetThreadInstance()->OnAcquireLock(sync);
    proto::LocalRecordInfo localv;
    comm::RetCode ret{GetLock(record_key_info.key(), localv)};
    if (comm::RetCode::RET_ERR_KEY_NOT_EXIST == ret) {
        QLVerb("storage %d key \"%s\" not exist", static_cast<int>(storage_type_),
               record_key_info.key().c_str());
        // TODO:
        //OssAttrInc(oss_attr_id_, 55u, 1u);
    } else if (comm::RetCode::RET_OK != ret) {
        QLErr("storage %d key \"%s\" err %d", static_cast<int>(storage_type_),
              record_key_info.key().c_str(), ret);
        // TODO:
        //OssAttrInc(oss_attr_id_, 54u, 1u);

        return ret;
    }

    if (comm::RetCode::RET_ERR_KEY_NOT_EXIST != ret &&
        localv.version() != record_key_info.version()) {
        QLErr("storage %d key \"%s\" local.ver %llu != req.ver %llu err",
              static_cast<int>(storage_type_), record_key_info.key().c_str(),
              localv.version(), record_key_info.version());
        // TODO:
        //OssAttrInc(oss_attr_id_, 56u, 1u);

        return comm::RetCode::RET_ERR_VERSION_NOT_EQUAL;
    }

    if (0 == v.lease_time_ms()) {
        QLVerb("storage %d key \"%s\" lease_time_ms %llu delete",
               static_cast<int>(storage_type_), record_key_info.key().c_str(), v.lease_time_ms());
        return DeleteLock(record_key_info.key(), sync);
    }

    // TODO: not copy?
    proto::LocalRecordInfo v2;
    v2.set_version(v.version());
    v2.set_value(v.value());
    v2.set_lease_time_ms(v.lease_time_ms());
    if (-1 != v2.lease_time_ms()) {
        v2.set_expire_time_ms(comm::utils::Time::GetSteadyClockMS() + v.lease_time_ms());
    }
    QLVerb("storage %d key \"%s\" ver %llu value \"%s\" "
           "lease_time_ms %llu expire_time_ms %llu",
           static_cast<int>(storage_type_), record_key_info.key().c_str(),
           v2.version(), v2.value().c_str(), v2.lease_time_ms(), v2.expire_time_ms());

    return SetLock(record_key_info.key(), v2, sync);
}

comm::RetCode LockDb::AcquireLock(const proto::RecordKeyInfo &record_key_info,
                                  const string &vstr, const bool sync) {
    if (StorageType::LEVELDB == storage_type_) {
        proto::LocalRecordInfo v;
        bool succ{v.ParseFromString(vstr)};
        if (!succ) {
            QLErr("storage %d ParseFromString err", static_cast<int>(storage_type_));

            return comm::RetCode::RET_ERR_LOGIC;
        }
        return AcquireLock(record_key_info, v, sync);
    } else {
        return comm::RetCode::RET_ERR_NO_IMPL;
    }

    return comm::RetCode::RET_OK;
}

//comm::RetCode LockDb::ReleaseLock(const phxqueue_proto::LockKeyInfo &lock_key_info, const bool sync) {
//    phxqueue_proto::LocalLockInfo localv;
//    comm::RetCode ret{Get(lock_key_info.lock_key(), localv)};
//    if (comm::RetCode::RET_ERR_KEY_NOT_EXIST == ret) {
//        QLVerb("storage %d lock_key \"%s\" not exist", static_cast<int>(storage_type_),
//               lock_key_info.lock_key().c_str());
//        // TODO:
//        //OssAttrInc(oss_attr_id_, 65u, 1u);
//    } else if (comm::RetCode::RET_OK != ret) {
//        QLErr("storage %d lock_key \"%s\" err %d", static_cast<int>(storage_type_),
//              lock_key_info.lock_key().c_str(), ret);
//        // TODO:
//        //OssAttrInc(oss_attr_id_, 64u, 1u);
//
//        return ret;
//    }
//
//    if (comm::RetCode::RET_ERR_KEY_NOT_EXIST != ret && localv.version() != lock_key_info.version()) {
//        QLErr("storage %d lock_key \"%s\" local.ver %llu != req.ver %llu err",
//              static_cast<int>(storage_type_), lock_key_info.lock_key().c_str(),
//              localv.version(), lock_key_info.version());
//        // TODO:
//        //OssAttrInc(oss_attr_id_, 66u, 1u);
//
//        return comm::RetCode::RET_ERR_VERSION_NOT_EQUAL;
//    }
//
//    if (StorageType::MAP == storage_type_) {
//        lock_map_.erase(lock_key_info.lock_key());
//    } else if (StorageType::LEVELDB == storage_type_) {
//        leveldb::WriteOptions write_options;
//        write_options.sync = sync;
//        leveldb::Status status(leveldb_->Delete(write_options, lock_key_info.lock_key()));
//        if (status.IsNotFound()) {
//            QLVerb("storage %d lock_key \"%s\" not exist", static_cast<int>(storage_type_),
//                   lock_key_info.lock_key().c_str());
//
//            return comm::RetCode::RET_ERR_KEY_NOT_EXIST;
//        }
//        if (!status.ok()) {
//            QLErr("storage %d err \"%s\"", static_cast<int>(storage_type_),
//                  status.ToString().c_str());
//
//            return comm::RetCode::RET_ERR_LEVELDB;
//        }
//    } else {
//        return comm::RetCode::RET_ERR_NO_IMPL;
//    }
//
//    return comm::RetCode::RET_OK;
//}

//comm::RetCode LockDb::CleanLock(const google::protobuf::RepeatedPtrField<phxqueue_proto::LockKeyInfo> &
//                          lock_key_infos, const bool sync) {
//    leveldb::WriteBatch batch;
//
//    for (auto &&lock_key_info : lock_key_infos) {
//        phxqueue_proto::LocalLockInfo localv;
//        comm::RetCode ret{Get(lock_key_info.lock_key(), localv)};
//        if (comm::RetCode::RET_ERR_KEY_NOT_EXIST != ret && comm::RetCode::RET_OK != ret) {
//            QLErr("storage %d lock_key \"%s\" err %d", static_cast<int>(storage_type_),
//                  lock_key_info.lock_key().c_str(), ret);
//
//            continue;
//        }
//
//        if (comm::RetCode::RET_ERR_KEY_NOT_EXIST != ret && localv.version() != lock_key_info.version()) {
//            QLErr("storage %d lock_key \"%s\" local.ver %llu != req.ver %llu err",
//                  static_cast<int>(storage_type_), lock_key_info.lock_key().c_str(),
//                  localv.version(), lock_key_info.version());
//
//            continue;
//        }
//
//        if (StorageType::MAP == storage_type_) {
//            lock_map_.erase(lock_key_info.lock_key());
//        } else if (StorageType::LEVELDB == storage_type_) {
//            batch.Delete(lock_key_info.lock_key());
//        } else {
//            return comm::RetCode::RET_ERR_NO_IMPL;
//        }
//    }
//
//    if (StorageType::LEVELDB == storage_type_) {
//        leveldb::WriteOptions write_options;
//        write_options.sync = sync;
//        leveldb::Status status(leveldb_->Write(write_options, &batch));
//        if (!status.ok()) {
//            QLErr("storage %d err \"%s\"", static_cast<int>(storage_type_), status.ToString().c_str());
//
//            return comm::RetCode::RET_ERR_LEVELDB;
//        }
//    }
//
//    return comm::RetCode::RET_OK;
//}

comm::RetCode LockDb::GetString(const string &k, proto::LocalRecordInfo &v) const {
    if (0 >= k.size()) {
        QLErr("storage %d key \"%s\" invalid", static_cast<int>(storage_type_), k.c_str());

        return comm::RetCode::RET_ERR_KEY;
    }

    return GetRecord(string(KEY_USER_STRING_PREFIX) + k, v);
}

comm::RetCode LockDb::SetString(const string &k, const proto::LocalRecordInfo &v, const bool sync) {
    if (0 >= k.size()) {
        QLErr("storage %d key \"%s\" invalid", static_cast<int>(storage_type_), k.c_str());

        return comm::RetCode::RET_ERR_KEY;
    }

    return SetRecord(string(KEY_USER_STRING_PREFIX) + k, v, sync);
}

comm::RetCode LockDb::DeleteString(const string &k, const bool sync) {
    if (0 >= k.size()) {
        QLErr("storage %d key \"%s\" invalid", static_cast<int>(storage_type_), k.c_str());

        return comm::RetCode::RET_ERR_KEY;
    }

    return DeleteRecord(string(KEY_USER_STRING_PREFIX) + k, sync);
}

comm::RetCode LockDb::GetLock(const string &k, proto::LocalRecordInfo &v) const {
    if (0 >= k.size()) {
        QLErr("storage %d key \"%s\" invalid", static_cast<int>(storage_type_), k.c_str());

        return comm::RetCode::RET_ERR_KEY;
    }

    return GetRecord(string(KEY_USER_LOCK_PREFIX) + k, v);
}

comm::RetCode LockDb::SetLock(const string &k, const proto::LocalRecordInfo &v, const bool sync) {
    if (0 >= k.size()) {
        QLErr("storage %d key \"%s\" invalid", static_cast<int>(storage_type_), k.c_str());

        return comm::RetCode::RET_ERR_KEY;
    }

    return SetRecord(string(KEY_USER_LOCK_PREFIX) + k, v, sync);
}

comm::RetCode LockDb::DeleteLock(const string &k, const bool sync) {
    if (0 >= k.size()) {
        QLErr("storage %d key \"%s\" invalid", static_cast<int>(storage_type_), k.c_str());

        return comm::RetCode::RET_ERR_KEY;
    }

    return DeleteRecord(string(KEY_USER_LOCK_PREFIX) + k, sync);
}

comm::RetCode LockDb::GetRecord(const string &k, proto::LocalRecordInfo &v) const {
    if (0 >= k.size()) {
        QLErr("storage %d key \"%s\" invalid", static_cast<int>(storage_type_), k.c_str());

        return comm::RetCode::RET_ERR_KEY;
    }

    if (StorageType::MAP == storage_type_) {
        auto it(map_.find(k));
        if (map_.end() == it) {
            QLVerb("storage %d key \"%s\" not exist", static_cast<int>(storage_type_), k.c_str());

            // not found
            return comm::RetCode::RET_ERR_KEY_NOT_EXIST;
        }
        v = it->second;
        QLVerb("storage %d key \"%s\" ver %llu", static_cast<int>(storage_type_), k.c_str(), v.version());
    } else if (StorageType::LEVELDB == storage_type_) {
        string vstr;
        comm::RetCode ret{DiskGet(k, vstr)};
        if (comm::RetCode::RET_OK != ret) {
            return ret;
        }
        bool succ(v.ParseFromString(vstr));
        if (!succ) {
            QLErr("storage %d ParseFromString err", static_cast<int>(storage_type_));

            return comm::RetCode::RET_ERR_PROTOBUF_PARSE;
        }
        QLVerb("storage %d key \"%s\" ver %llu", static_cast<int>(storage_type_), k.c_str(), v.version());
    } else {
        return comm::RetCode::RET_ERR_NO_IMPL;
    }

    return comm::RetCode::RET_OK;
}

comm::RetCode LockDb::SetRecord(const string &k, const proto::LocalRecordInfo &v, const bool sync) {
    if (0 >= k.size()) {
        QLErr("storage %d key \"%s\" invalid", static_cast<int>(storage_type_), k.c_str());

        return comm::RetCode::RET_ERR_KEY;
    }

    if (StorageType::MAP == storage_type_) {
        map_[k] = v;
        QLVerb("storage %d key \"%s\" lease_time_ms %llu",
               static_cast<int>(storage_type_), k.c_str(), v.lease_time_ms());
    } else if (StorageType::LEVELDB == storage_type_) {
        string vstr;
        // TODO: Not copy?
        proto::LocalRecordInfo local_record_info2 = v;
        local_record_info2.clear_expire_time_ms();
        bool succ{local_record_info2.SerializeToString(&vstr)};
        if (!succ) {
            QLErr("storage %d SerializeToString err", static_cast<int>(storage_type_));

            return comm::RetCode::RET_ERR_PROTOBUF_SERIALIZE;
        }
        QLVerb("storage %d key \"%s\" lease_time_ms %llu",
               static_cast<int>(storage_type_), k.c_str(), v.lease_time_ms());

        return DiskSet(k, vstr, sync);
    } else {
        return comm::RetCode::RET_ERR_NO_IMPL;
    }

    return comm::RetCode::RET_OK;
}

comm::RetCode LockDb::DeleteRecord(const string &k, const bool sync) {
    if (0 >= k.size()) {
        QLErr("storage %d key \"%s\" invalid", static_cast<int>(storage_type_), k.c_str());

        return comm::RetCode::RET_ERR_KEY;
    }

    if (StorageType::MAP == storage_type_) {
        map_.erase(k);
        QLVerb("key \"%s\"", k.c_str());
    } else if (StorageType::LEVELDB == storage_type_) {
        QLVerb("key \"%s\"", k.c_str());

        return DiskDelete(k, sync);
    } else {
        return comm::RetCode::RET_ERR_NO_IMPL;
    }

    return comm::RetCode::RET_OK;
}

comm::RetCode LockDb::DiskGet(const string &k, string &vstr) const {
    if (0 >= k.size()) {
        QLErr("key \"%s\" invalid", k.c_str());

        return comm::RetCode::RET_ERR_KEY;
    }

    if (StorageType::LEVELDB == storage_type_) {
        leveldb::ReadOptions options;
        // force checksum verification
        options.verify_checksums = leveldb_checks_;
        leveldb::Status status(leveldb_->Get(options, k, &vstr));
        if (status.IsNotFound()) {
            QLVerb("key \"%s\" not exist", k.c_str());

            return comm::RetCode::RET_ERR_KEY_NOT_EXIST;
        }
        if (!status.ok()) {
            QLErr("err \"%s\"", status.ToString().c_str());

            return comm::RetCode::RET_ERR_LEVELDB;
        }
    } else {
        return comm::RetCode::RET_ERR_NO_IMPL;
    }

    return comm::RetCode::RET_OK;
}

comm::RetCode LockDb::DiskSet(const string &k, const string &vstr, const bool sync) {
    if (0 >= k.size()) {
        QLErr("key \"%s\" invalid", k.c_str());

        return comm::RetCode::RET_ERR_KEY;
    }

    if (StorageType::LEVELDB == storage_type_) {
        leveldb::WriteOptions write_options;
        write_options.sync = sync;
        leveldb::Status status(leveldb_->Put(write_options, k, vstr));
        if (!status.ok()) {
            QLErr("err \"%s\"", status.ToString().c_str());

            return comm::RetCode::RET_ERR_LEVELDB;
        }
    } else {
        return comm::RetCode::RET_ERR_NO_IMPL;
    }

    return comm::RetCode::RET_OK;
}

comm::RetCode LockDb::DiskDelete(const string &k, const bool sync) {
    if (0 >= k.size()) {
        QLErr("key \"%s\" invalid", k.c_str());

        return comm::RetCode::RET_ERR_KEY;
    }

    if (StorageType::LEVELDB == storage_type_) {
        leveldb::WriteOptions write_options;
        write_options.sync = sync;
        leveldb::Status status(leveldb_->Delete(write_options, k));
        if (status.IsNotFound()) {
            QLVerb("key \"%s\" not exist", k.c_str());

            return comm::RetCode::RET_ERR_KEY_NOT_EXIST;
        }
        if (!status.ok()) {
            QLErr("err \"%s\"", status.ToString().c_str());

            return comm::RetCode::RET_ERR_LEVELDB;
        }
        QLVerb("key \"%s\"", k.c_str());
    } else {
        return comm::RetCode::RET_ERR_NO_IMPL;
    }

    return comm::RetCode::RET_OK;
}

comm::RetCode LockDb::Sync() {
    if (StorageType::MAP == storage_type_) {
    } else if (StorageType::LEVELDB == storage_type_) {
        leveldb::WriteOptions write_options;
        write_options.sync = true;
        leveldb::Status status(leveldb_->Put(write_options, KEY_IGNORE_SYNC_TRIGGER, ""));
        if (!status.ok()) {
            QLErr("%s", status.ToString().c_str());

            return comm::RetCode::RET_ERR_LEVELDB;
        }
        QLVerb("key \"%s\"", KEY_IGNORE_SYNC_TRIGGER);
    } else {
        return comm::RetCode::RET_ERR_NO_IMPL;
    }

    return comm::RetCode::RET_OK;
}

void LockDb::SeekToFirstRecord() {
    if (StorageType::MAP == storage_type_) {
        map_it_ = map_.cbegin();
    } else if (StorageType::LEVELDB == storage_type_) {
        DiskSeekToFirst();
    }
}

void LockDb::NextRecord() {
    if (StorageType::MAP == storage_type_) {
        ++map_it_;
    } else if (StorageType::LEVELDB == storage_type_) {
        DiskNext();
    }
}

bool LockDb::ValidRecord() const {
    if (StorageType::MAP == storage_type_) {
        return map_.end() != map_it_;
    } else if (StorageType::LEVELDB == storage_type_) {
        return DiskValid();
    }

    return false;
}

comm::RetCode LockDb::GetCurrentRecord(string *const k, string *const vstr,
                                       proto::LocalRecordInfo *const local_record_info) {
    if (StorageType::MAP == storage_type_) {
        *k = map_it_->first;
        *local_record_info = map_it_->second;
    } else if (StorageType::LEVELDB == storage_type_) {
        DataType data_type{DataType::NONE};
        comm::RetCode ret{DiskGetCurrent(k, vstr, &data_type)};
        if (comm::RetCode::RET_OK != ret) {
            QLErr("storage %d key \"%s\" get value err",
                  static_cast<int>(storage_type_), k->c_str());

            return ret;
        }

        if (DataType::IGNORE == data_type) {
            // reserve key
            return comm::RetCode::RET_KEY_IGNORE;
        }

        bool succ{local_record_info->ParseFromString(*vstr)};
        if (!succ) {
            QLErr("storage %d ParseFromString key \"%s\" value.size \"%s\" err",
                  static_cast<int>(storage_type_), k->c_str(), vstr->size());

            return comm::RetCode::RET_ERR_LOGIC;
        }
    } else {
        return comm::RetCode::RET_ERR_NO_IMPL;
    }

    return comm::RetCode::RET_OK;
}

void LockDb::DiskSeekToFirst() {
    if (StorageType::LEVELDB == storage_type_) {
        leveldb::ReadOptions options;
        // force checksum verification
        options.verify_checksums = leveldb_checks_;
        leveldb_it_ = leveldb_->NewIterator(options);
        leveldb_it_->SeekToFirst();
    }
}

void LockDb::DiskNext() {
    if (StorageType::LEVELDB == storage_type_) {
        leveldb_it_->Next();
    }
}

bool LockDb::DiskValid() const {
    if (StorageType::LEVELDB == storage_type_) {
        return leveldb_it_->Valid();
    }

    return false;
}

comm::RetCode LockDb::DiskGetCurrent(string *const k, string *const vstr,
                                     DataType *const data_type) {
    if (StorageType::LEVELDB == storage_type_) {
        *k = leveldb_it_->key().ToString();
        *vstr = leveldb_it_->value().ToString();

        if (0u == k->find(KEY_IGNORE_PREFIX)) {
            *data_type = DataType::IGNORE;
        } else {
            *data_type = DataType::USER;
        }
    } else {
        return comm::RetCode::RET_ERR_NO_IMPL;
    }

    return comm::RetCode::RET_OK;
}

comm::RetCode LockDb::ForwardLoopKey(string *const k) {
    if (StorageType::MAP == storage_type_) {
        if (0 >= map_.size())
            return comm::RetCode::RET_ERR_RANGE;
        auto it(map_.find(prev_loop_key_));
        if (map_.end() == it) {
            *k = prev_loop_key_ = map_.begin()->first;

            return comm::RetCode::RET_OK;
        }

        it = map_.upper_bound(prev_loop_key_);
        if (map_.end() == it)
            it = map_.begin();
        prev_loop_key_ = it->first;
        *k = prev_loop_key_;
    } else {
        return comm::RetCode::RET_ERR_NO_IMPL;
    }

    return comm::RetCode::RET_OK;
}


}  // namespace lock

}  // namespace phxqueue

