#include "phxqueue/store/checkpointstat.h"

#include <cstdio>
#include <cstring>
#include <fcntl.h>
#include <mutex>
#include <sys/mman.h>
#include <sys/shm.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <time.h>
#include <unistd.h>

#include "phxqueue/comm.h"


#define CHECKPOINTSTAT_MAGIC 9970256


namespace phxqueue {

namespace store {


using namespace std;


struct CheckPointStatItem_t {
    uint32_t magic;
    uint64_t cp;
    char reserved[20];
};


class CheckPointStat::CheckPointStatImpl {
  public:
    CheckPointStatImpl() {}
    virtual ~CheckPointStatImpl() {}

    string dir;
    string file;
    mutex lock;

    uint32_t len{0};
    CheckPointStatItem_t *buf{nullptr};
};


CheckPointStat::CheckPointStat() : impl_(new CheckPointStatImpl()) {}

CheckPointStat::~CheckPointStat() {
    if (impl_->buf) munmap(impl_->buf, impl_->len);
}

comm::RetCode CheckPointStat::Init(const string &dir, const string &file) {
    if (dir.empty() || file.empty())
        return comm::RetCode::RET_ERR_ARG;

    impl_->dir = dir;
    impl_->file = file;
    string path{impl_->dir + impl_->file};

    impl_->buf = nullptr;
    impl_->len = sizeof(CheckPointStatItem_t);

    int fd = open(path.c_str(), O_RDWR | O_CREAT, S_IRUSR | S_IWUSR);
    if (fd < 0) {
        QLErr("open err path %s %s", path.c_str(), strerror(errno));
        return comm::RetCode::RET_ERR_SYS;
    }

    if (ftruncate(fd, (off_t)impl_->len) < 0) {
        QLErr("ftruncate err %s", strerror(errno));
        close(fd);
        return comm::RetCode::RET_ERR_SYS;
    }

    void *pa{mmap(nullptr, impl_->len, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0)};

    close(fd);

    if ((char *)pa == MAP_FAILED) {
        QLErr("mmap err %s", strerror(errno));
        return comm::RetCode::RET_ERR_SYS;
    }

    impl_->buf = (CheckPointStatItem_t*)pa;

    return comm::RetCode::RET_OK;
}

comm::RetCode CheckPointStat::GetCheckPoint(uint64_t &cp) {
    cp = -1;

    lock_guard<mutex> lock_guard(impl_->lock);

    CheckPointStatItem_t *item = impl_->buf;
    if (item->magic == CHECKPOINTSTAT_MAGIC) cp = item->cp;
    return comm::RetCode::RET_OK;
}

comm::RetCode CheckPointStat::UpdateCheckPointAndFlush(const uint64_t cp) {
    lock_guard<mutex> lock_guard(impl_->lock);

    CheckPointStatItem_t *item = impl_->buf;
    if (item->magic != CHECKPOINTSTAT_MAGIC) item->magic = CHECKPOINTSTAT_MAGIC;
    item->cp = cp;

    const size_t sync_size{offsetof(CheckPointStatItem_t, reserved)};
    int ret = msync(item, sync_size, MS_SYNC);
    if (0 != ret) {
        return comm::RetCode::RET_ERR_SYS;
    }
    return comm::RetCode::RET_OK;
}


string CheckPointStat::GetDir() const {
    return impl_->dir;
}

string CheckPointStat::GetFile() const {
    return impl_->file;
}


class CheckPointStatMgr::CheckPointStatMgrImpl {
  public:
    CheckPointStatMgrImpl() {}
    virtual ~CheckPointStatMgrImpl() {}

    Store *store{nullptr};
    int nstat{0};
    unique_ptr<CheckPointStat[]> stats{nullptr};
};

CheckPointStatMgr::CheckPointStatMgr(Store *const store) : impl_(new CheckPointStatMgrImpl()) {
    impl_->store = store;
}

CheckPointStatMgr::~CheckPointStatMgr() {}


comm::RetCode CheckPointStatMgr::Init() {
    auto &&opt = impl_->store->GetStoreOption();

    impl_->nstat = opt->ngroup;
    if (!impl_->nstat) return comm::RetCode::RET_OK;

    comm::RetCode ret;

    impl_->stats.reset(new CheckPointStat[impl_->nstat]);

    auto cp_path(opt->data_dir_path + "/cp/");
    if (!comm::utils::CreateDir(cp_path)) {
        QLErr("cp_path %s not exist", cp_path.c_str());
        return comm::RetCode::RET_DIR_NOT_EXIST;
    }
    for (int i{0}; i < impl_->nstat; ++i) {
        if (comm::RetCode::RET_OK != (ret = impl_->stats[i].Init(cp_path, to_string(i)))) {
            return ret;
        }
    }

    return comm::RetCode::RET_OK;
}

CheckPointStat *CheckPointStatMgr::GetCheckPointStat(const int paxos_group_id) {
    if (paxos_group_id >= impl_->nstat) return nullptr;
    return &impl_->stats[paxos_group_id];
}


}  // namespace store

}  // namespace phxqueue


//gzrd_Lib_CPP_Version_ID--start
#ifndef GZRD_SVN_ATTR
#define GZRD_SVN_ATTR "0"
#endif
static char gzrd_Lib_CPP_Version_ID[] __attribute__((used))="$HeadURL: http://scm-gy.tencent.com/gzrd/gzrd_mail_rep/phoenix_proj/trunk/phoenix/phxqueue/store/paxos/checkpointstat.cpp $ $Id: checkpointstat.cpp 1832337 2016-11-22 06:57:52Z unixliang $ " GZRD_SVN_ATTR "__file__";
// gzrd_Lib_CPP_Version_ID--end

