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
    enum class Type {MAP, LEVELDB};

    LockDb();

    // noncopyable
    LockDb(const LockDb &) = delete;

    LockDb(LockDb &&rhs);

    virtual ~LockDb();

    // noncopyable
    LockDb &operator=(const LockDb &) = delete;

    LockDb &operator=(LockDb &&rhs);

    comm::RetCode Init(const Type type, const std::string &leveldb_file);
    comm::RetCode Dispose();

    comm::RetCode AcquireLock(const proto::LockKeyInfo &lock_key_info,
                              const proto::LocalLockInfo &v, const bool sync = true);
    comm::RetCode AcquireLock(const proto::LockKeyInfo &lock_key_info,
                              const std::string &vstr, const bool sync = true);

//    comm::RetCode ReleaseLock(const phxqueue_proto::LockKeyInfo &lock_key_info, const bool sync = true);

//    comm::RetCode CleanLock(const google::protobuf::RepeatedPtrField<phxqueue_proto::LockKeyInfo> &
//                      lock_key_infos, const bool sync = true);

    comm::RetCode Put(const std::string &k, const proto::LocalLockInfo &v,
                const bool sync = true);
    comm::RetCode Put(const std::string &k, const std::string &vstr, const bool sync = true);

    comm::RetCode Get(const std::string &k, proto::LocalLockInfo &v);
    comm::RetCode Get(const std::string &k, std::string &vstr);

    comm::RetCode Delete(const std::string &k, const bool sync = true);

    inline int GetSize() { return Type::MAP == type_ ? map_.size() : 0; }

    void SeekToFirst();
    void Next();
    bool Valid();
    comm::RetCode GetCurrent(std::string &k, proto::LocalLockInfo &v);
    comm::RetCode GetCurrent(std::string &k, std::string &vstr);

    comm::RetCode CleanForward(std::string &k);

    inline pthread_mutex_t *const mutex() { return mutex_; }

  private:
    pthread_mutex_t *mutex_{nullptr};
    Type type_{Type::MAP};
    std::map<std::string, proto::LocalLockInfo> map_;
    std::map<std::string, proto::LocalLockInfo>::const_iterator map_it_;
    leveldb::DB *leveldb_{nullptr};
    leveldb::Iterator *leveldb_it_{nullptr};
    bool leveldb_checks_{true};
    std::string current_clean_key_;
    // must modify move constructor & move assignment at the same time
};


}  // namespace lock

}  // namespace phxqueue

