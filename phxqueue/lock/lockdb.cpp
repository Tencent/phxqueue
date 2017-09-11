#include "phxqueue/lock/lockdb.h"

#include "leveldb/write_batch.h"

#include "phxqueue/comm.h"


namespace {


// for inner use only, user cannot use key with this prefix
constexpr char *KEY_IGNORE_PREFIX{"__ignore__"};


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
    type_ = rhs.type_;
    map_ = move(rhs.map_);
    map_it_ = move(rhs.map_it_);
    leveldb_ = rhs.leveldb_;
    rhs.leveldb_ = nullptr;
    leveldb_it_ = rhs.leveldb_it_;
    rhs.leveldb_it_ = nullptr;
    leveldb_checks_ = rhs.leveldb_checks_;
    current_clean_key_ = move(rhs.current_clean_key_);
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
    type_ = rhs.type_;
    map_ = move(rhs.map_);
    map_it_ = move(rhs.map_it_);
    leveldb_ = rhs.leveldb_;
    rhs.leveldb_ = nullptr;
    leveldb_it_ = rhs.leveldb_it_;
    rhs.leveldb_it_ = nullptr;
    leveldb_checks_ = rhs.leveldb_checks_;
    current_clean_key_ = move(rhs.current_clean_key_);

    return *this;
}

comm::RetCode LockDb::Init(const LockDb::Type type, const string &leveldb_file) {
    type_ = type;

    if (Type::MAP == type_) {
    } else if (Type::LEVELDB == type_) {
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
    if (Type::MAP == type_) {
        map_.clear();
    } else if (Type::LEVELDB == type_) {
        if (nullptr != leveldb_) {
            delete leveldb_;
            leveldb_ = nullptr;
        }
    } else {
        return comm::RetCode::RET_ERR_NO_IMPL;
    }

    return comm::RetCode::RET_OK;
}

comm::RetCode LockDb::AcquireLock(const proto::LockKeyInfo &lock_key_info,
                                  const proto::LocalLockInfo &v, const bool sync) {
    comm::LockDbBP::GetThreadInstance()->OnAcquireLock(sync);
    proto::LocalLockInfo localv;
    comm::RetCode ret{Get(lock_key_info.lock_key(), localv)};
    if (comm::RetCode::RET_ERR_KEY_NOT_EXIST == ret) {
        QLVerb("lock_key \"%s\" not exist", lock_key_info.lock_key().c_str());
        // TODO:
        //OssAttrInc(oss_attr_id_, 55u, 1u);
    } else if (comm::RetCode::RET_OK != ret) {
        QLErr("lock_key \"%s\" err %d", lock_key_info.lock_key().c_str(), ret);
        // TODO:
        //OssAttrInc(oss_attr_id_, 54u, 1u);

        return ret;
    }

    if (comm::RetCode::RET_ERR_KEY_NOT_EXIST != ret && localv.version() != lock_key_info.version()) {
        QLErr("lock_key \"%s\" local.ver %llu != req.ver %llu err",
                  lock_key_info.lock_key().c_str(),
                  localv.version(), lock_key_info.version());
        // TODO:
        //OssAttrInc(oss_attr_id_, 56u, 1u);

        return comm::RetCode::RET_ERR_VERSION_NOT_EQUAL;
    }

    if (Type::MAP == type_) {
        if (0 >= v.lease_time_ms()) {  // release lock
            map_.erase(lock_key_info.lock_key());
            QLVerb("lock_key \"%s\" erase", lock_key_info.lock_key().c_str());

            return comm::RetCode::RET_OK;
        }

        // TODO: not copy?
        proto::LocalLockInfo v2;
        v2.set_version(v.version());
        v2.set_client_id(v.client_id());
        v2.set_lease_time_ms(v.lease_time_ms());
        v2.set_expire_time_ms(comm::utils::Time::GetSteadyClockMS() + v.lease_time_ms());
        QLVerb("lock_key \"%s\" ver %llu client_id \"%s\" expire_time_ms %llu",
               lock_key_info.lock_key().c_str(), v2.version(),
               v2.client_id().c_str(), v2.expire_time_ms());

        return Put(lock_key_info.lock_key(), v2);

    } else if (Type::LEVELDB == type_) {
        if (0 >= v.lease_time_ms()) {  // release lock
            leveldb::WriteOptions write_options;
            write_options.sync = sync;
            leveldb::Status status(leveldb_->Delete(write_options, lock_key_info.lock_key()));
            if (status.IsNotFound()) {
                QLVerb("lock_key \"%s\" not exist", lock_key_info.lock_key().c_str());

                return comm::RetCode::RET_ERR_KEY_NOT_EXIST;
            }
            if (!status.ok()) {
                QLErr("%s", status.ToString().c_str());

                return comm::RetCode::RET_ERR_LEVELDB;
            }

            return comm::RetCode::RET_OK;
        }

        // TODO: not copy?
        proto::LocalLockInfo v2;
        v2.set_version(v.version());
        v2.set_client_id(v.client_id());
        v2.set_lease_time_ms(v.lease_time_ms());
        QLVerb("lock_key \"%s\" ver %llu client_id \"%s\"", lock_key_info.lock_key().c_str(),
               v2.version(), v2.client_id().c_str());

        return Put(lock_key_info.lock_key(), v2, sync);

    } else {
        return comm::RetCode::RET_ERR_NO_IMPL;

    }

    return comm::RetCode::RET_OK;
}

comm::RetCode LockDb::AcquireLock(const proto::LockKeyInfo &lock_key_info,
                                  const string &vstr, const bool sync) {
    if (Type::LEVELDB == type_) {
        proto::LocalLockInfo v;
        bool succ{v.ParseFromString(vstr)};
        if (!succ) {
            QLErr("ParseFromString err");

            return comm::RetCode::RET_ERR_LOGIC;
        }
        return AcquireLock(lock_key_info, v, sync);
    } else {
        return comm::RetCode::RET_ERR_NO_IMPL;
    }

    return comm::RetCode::RET_OK;
}

//comm::RetCode LockDb::ReleaseLock(const phxqueue_proto::LockKeyInfo &lock_key_info, const bool sync) {
//    phxqueue_proto::LocalLockInfo localv;
//    comm::RetCode ret{Get(lock_key_info.lock_key(), localv)};
//    if (comm::RetCode::RET_ERR_KEY_NOT_EXIST == ret) {
//        QLVerb("lock_key \"%s\" not exist", lock_key_info.lock_key().c_str());
//        // TODO:
//        //OssAttrInc(oss_attr_id_, 65u, 1u);
//    } else if (comm::RetCode::RET_OK != ret) {
//        QLErr("lock_key \"%s\" err %d", lock_key_info.lock_key().c_str(), ret);
//        // TODO:
//        //OssAttrInc(oss_attr_id_, 64u, 1u);
//
//        return ret;
//    }
//
//    if (comm::RetCode::RET_ERR_KEY_NOT_EXIST != ret && localv.version() != lock_key_info.version()) {
//        QLErr("lock_key \"%s\" local.ver %llu != req.ver %llu err",
//              lock_key_info.lock_key().c_str(),
//              localv.version(), lock_key_info.version());
//        // TODO:
//        //OssAttrInc(oss_attr_id_, 66u, 1u);
//
//        return comm::RetCode::RET_ERR_VERSION_NOT_EQUAL;
//    }
//
//    if (Type::MAP == type_) {
//        map_.erase(lock_key_info.lock_key());
//    } else if (Type::LEVELDB == type_) {
//        leveldb::WriteOptions write_options;
//        write_options.sync = sync;
//        leveldb::Status status(leveldb_->Delete(write_options, lock_key_info.lock_key()));
//        if (status.IsNotFound()) {
//            QLVerb("lock_key \"%s\" not exist", lock_key_info.lock_key().c_str());
//
//            return comm::RetCode::RET_ERR_KEY_NOT_EXIST;
//        }
//        if (!status.ok()) {
//            QLErr("%s", status.ToString().c_str());
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
//            QLErr("lock_key \"%s\" err %d", lock_key_info.lock_key().c_str(), ret);
//
//            continue;
//        }
//
//        if (comm::RetCode::RET_ERR_KEY_NOT_EXIST != ret && localv.version() != lock_key_info.version()) {
//            QLErr("lock_key \"%s\" local.ver %llu != req.ver %llu err", lock_key_info.lock_key().c_str(),
//                  localv.version(), lock_key_info.version());
//
//            continue;
//        }
//
//        if (Type::MAP == type_) {
//            map_.erase(lock_key_info.lock_key());
//        } else if (Type::LEVELDB == type_) {
//            batch.Delete(lock_key_info.lock_key());
//        } else {
//            return comm::RetCode::RET_ERR_NO_IMPL;
//        }
//    }
//
//    if (Type::LEVELDB == type_) {
//        leveldb::WriteOptions write_options;
//        write_options.sync = sync;
//        leveldb::Status status(leveldb_->Write(write_options, &batch));
//        if (!status.ok()) {
//            QLErr("%s", status.ToString().c_str());
//
//            return comm::RetCode::RET_ERR_LEVELDB;
//        }
//    }
//
//    return comm::RetCode::RET_OK;
//}

comm::RetCode LockDb::Put(const string &k, const proto::LocalLockInfo &v, const bool sync) {
    if (0 >= k.size()) {
        QLErr("key \"%s\" invalid", k.c_str());

        return comm::RetCode::RET_ERR_KEY;
    }

    if (Type::MAP == type_) {
        // TODO: emplace?
        map_[k] = v;
        QLVerb("key \"%s\" ver %llu", k.c_str(), v.version());
    } else if (Type::LEVELDB == type_) {
        string vstr;
        // TODO: Not copy?
        proto::LocalLockInfo v2 = v;
        v2.clear_expire_time_ms();
        bool succ{v2.SerializeToString(&vstr)};
        if (!succ) {
            QLErr("SerializeToString err");

            return comm::RetCode::RET_ERR_LOGIC;
        }
        QLVerb("key \"%s\" ver %llu", k.c_str(), v.version());

        return Put(k, vstr, sync);
    } else {
        return comm::RetCode::RET_ERR_NO_IMPL;
    }

    return comm::RetCode::RET_OK;
}

comm::RetCode LockDb::Put(const string &k, const string &vstr, const bool sync) {
    if (0 >= k.size()) {
        QLErr("key \"%s\" invalid", k.c_str());

        return comm::RetCode::RET_ERR_KEY;
    }

    if (Type::LEVELDB == type_) {
        leveldb::WriteOptions write_options;
        write_options.sync = sync;
        leveldb::Status status(leveldb_->Put(write_options, k, vstr));
        if (!status.ok()) {
            QLErr("%s", status.ToString().c_str());

            return comm::RetCode::RET_ERR_LEVELDB;
        }
    } else {
        return comm::RetCode::RET_ERR_NO_IMPL;
    }

    return comm::RetCode::RET_OK;
}

comm::RetCode LockDb::Get(const string &k, proto::LocalLockInfo &v) {
    if (0 >= k.size()) {
        QLErr("key \"%s\" invalid", k.c_str());

        return comm::RetCode::RET_ERR_KEY;
    }

    if (Type::MAP == type_) {
        auto it(map_.find(k));
        if (map_.end() == it) {
            QLVerb("key \"%s\" not exist", k.c_str());

            // not found
            return comm::RetCode::RET_ERR_KEY_NOT_EXIST;
        }
        v = it->second;
        QLVerb("key \"%s\" ver %llu", k.c_str(), v.version());
    } else if (Type::LEVELDB == type_) {
        string vstr;
        comm::RetCode ret{Get(k, vstr)};
        if (comm::RetCode::RET_OK != ret) {
            return ret;
        }
        bool succ(v.ParseFromString(vstr));
        if (!succ) {
            QLErr("ParseFromString err");

            return comm::RetCode::RET_ERR_LOGIC;
        }
        QLVerb("key \"%s\" ver %llu", k.c_str(), v.version());
    } else {
        return comm::RetCode::RET_ERR_NO_IMPL;
    }

    return comm::RetCode::RET_OK;
}

comm::RetCode LockDb::Get(const string &k, string &vstr) {
    if (0 >= k.size()) {
        QLErr("key \"%s\" invalid", k.c_str());

        return comm::RetCode::RET_ERR_KEY;
    }

    if (Type::LEVELDB == type_) {
        leveldb::ReadOptions options;
        // force checksum verification
        options.verify_checksums = leveldb_checks_;
        leveldb::Status status(leveldb_->Get(options, k, &vstr));
        if (status.IsNotFound()) {
            QLVerb("key \"%s\" not exist", k.c_str());

            return comm::RetCode::RET_ERR_KEY_NOT_EXIST;
        }
        if (!status.ok()) {
            QLErr("%s", status.ToString().c_str());

            return comm::RetCode::RET_ERR_LEVELDB;
        }
    } else {
        return comm::RetCode::RET_ERR_NO_IMPL;
    }

    return comm::RetCode::RET_OK;
}

comm::RetCode LockDb::Delete(const string &k, const bool sync) {
    if (0 >= k.size()) {
        QLErr("key \"%s\" invalid", k.c_str());

        return comm::RetCode::RET_ERR_KEY;
    }

    if (Type::MAP == type_) {
        map_.erase(k);
        QLVerb("key \"%s\"", k.c_str());
    } else if (Type::LEVELDB == type_) {
        leveldb::WriteOptions write_options;
        write_options.sync = sync;
        leveldb::Status status(leveldb_->Delete(write_options, k));
        if (!status.ok()) {
            QLErr("%s", status.ToString().c_str());

            return comm::RetCode::RET_ERR_LEVELDB;
        }
        QLVerb("key \"%s\"", k.c_str());
    } else {
        return comm::RetCode::RET_ERR_NO_IMPL;
    }

    return comm::RetCode::RET_OK;
}

void LockDb::SeekToFirst() {
    if (Type::MAP == type_) {
        map_it_ = map_.cbegin();
    } else if (Type::LEVELDB == type_) {
        leveldb::ReadOptions options;
        // force checksum verification
        options.verify_checksums = leveldb_checks_;
        leveldb_it_ = leveldb_->NewIterator(options);
        leveldb_it_->SeekToFirst();
    }
}

void LockDb::Next() {
    if (Type::MAP == type_) {
        ++map_it_;
    } else if (Type::LEVELDB == type_) {
        leveldb_it_->Next();
    }
}

bool LockDb::Valid() {
    if (Type::MAP == type_) {
        return map_.end() != map_it_;
    } else if (Type::LEVELDB == type_) {
        return leveldb_it_->Valid();
    }

    return false;
}

comm::RetCode LockDb::GetCurrent(string &k, proto::LocalLockInfo &v) {
    if (Type::MAP == type_) {
        k = map_it_->first;
        v = map_it_->second;
    } else if (Type::LEVELDB == type_) {
        string vstr;
        comm::RetCode ret{GetCurrent(k, vstr)};
        if (comm::RetCode::RET_OK != ret) {
            QLErr("key \"%s\" get value err", k.c_str());

            return ret;
        }

        if (0u == k.find(KEY_IGNORE_PREFIX)) {
            // reserve key
            return comm::RetCode::RET_ERR_IGNORE;
        }

        bool succ{v.ParseFromString(vstr)};
        if (!succ) {
            QLErr("ParseFromString key \"%s\" value \"%s\" err", k.c_str(), vstr.c_str());

            return comm::RetCode::RET_ERR_LOGIC;
        }
    } else {
        return comm::RetCode::RET_ERR_NO_IMPL;
    }

    return comm::RetCode::RET_OK;
}

comm::RetCode LockDb::GetCurrent(string &k, string &vstr) {
    if (Type::LEVELDB == type_) {
        k = leveldb_it_->key().ToString();
        vstr = leveldb_it_->value().ToString();
    } else {
        return comm::RetCode::RET_ERR_NO_IMPL;
    }

    return comm::RetCode::RET_OK;
}

comm::RetCode LockDb::CleanForward(string &k) {
    if (Type::MAP == type_) {
        if (0 >= map_.size())
            return comm::RetCode::RET_OK;
        k = current_clean_key_;
        auto it(map_.upper_bound(current_clean_key_));
        if (map_.end() == it)
            it = map_.begin();
        current_clean_key_ = it->first;
    } else {
        return comm::RetCode::RET_ERR_NO_IMPL;
    }

    return comm::RetCode::RET_OK;
}


}  // namespace lock

}  // namespace phxqueue


//gzrd_Lib_CPP_Version_ID--start
#ifndef GZRD_SVN_ATTR
#define GZRD_SVN_ATTR "0"
#endif
static char gzrd_Lib_CPP_Version_ID[] __attribute__((used))="$HeadURL: http://scm-gy.tencent.com/gzrd/gzrd_mail_rep/phoenix_proj/trunk/phxqueue/src/lock/lockdb.cpp $ $Id: lockdb.cpp 2107966 2017-06-01 07:36:22Z walnuthe $ " GZRD_SVN_ATTR "__file__";
// gzrd_Lib_CPP_Version_ID--end

