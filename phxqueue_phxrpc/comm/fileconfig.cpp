#include "phxqueue_phxrpc/comm/fileconfig.h"

#include <assert.h>
#include <cstring>
#include <map>
#include <sys/stat.h>

#include "phxqueue/comm.h"


namespace phxqueue_phxrpc {

namespace comm {


using namespace std;


class FileConfig::FileConfigImpl {
  public:
    string file;
    bool read{false};
    pthread_rwlock_t rwlock;
    string content;
};

FileConfig::FileConfig() : impl_(new FileConfigImpl()) {
    assert(impl_);
    pthread_rwlock_init(&impl_->rwlock, NULL);
}

FileConfig::FileConfig(const std::string &file) : impl_(new FileConfigImpl()) {
    assert(impl_);
    impl_->file = file;
    pthread_rwlock_init(&impl_->rwlock, NULL);
}

FileConfig::~FileConfig() {
}

void FileConfig::SetFile(const string &file) {
    phxqueue::comm::utils::RWLock rwlock_write(&impl_->rwlock,
                                               phxqueue::comm::utils::RWLock::LockMode::WRITE);

    impl_->file = file;
    impl_->read = false;
}


bool FileConfig::Read() {
    phxqueue::comm::utils::RWLock rwlock_write(&impl_->rwlock,
                                               phxqueue::comm::utils::RWLock::LockMode::WRITE);

    if (impl_->read) return true;
    if (impl_->file.empty()) return false;

    bool ret = true;
    FILE *fp = fopen(impl_->file.c_str(), "r");
    if (fp) {
        struct stat fileStat;
        if (0 == fstat( fileno( fp ), &fileStat)) {
            unique_ptr<char[]> buf = unique_ptr<char[]>(new char[fileStat.st_size + 64]);

            fread(buf.get(), fileStat.st_size, 1, fp );
            buf[fileStat.st_size] = '\0';
            impl_->content = string(buf.get(), fileStat.st_size);

            ParseContent(impl_->content);
        } else {
            ret = false;
            //QLErr("fstat fail. file %s errno %d %s", impl_->file.c_str(), errno, strerror(errno));
        }

        fclose(fp); fp = nullptr;
    } else {
        ret = false;
        //QLErr("open fial. file %s errno %d %s", impl_->file.c_str(), errno, strerror(errno));
    }

    if (ret) impl_->read = true;

    return ret;
}


bool FileConfig::GetContent(string &content) {
    if (!impl_->read) return false;
    content = impl_->content;
    return true;
}


}  // namespace comm

}  // namespace phxqueue_phxrpc


//gzrd_Lib_CPP_Version_ID--start
#ifndef GZRD_SVN_ATTR
#define GZRD_SVN_ATTR "0"
#endif
static char gzrd_Lib_CPP_Version_ID[] __attribute__((used))="$HeadURL$ $Id$ " GZRD_SVN_ATTR "__file__";
// gzrd_Lib_CPP_Version_ID--end

