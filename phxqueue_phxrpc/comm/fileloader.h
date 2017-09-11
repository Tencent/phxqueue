#pragma once

#include <memory>
#include <string>
#include <sys/stat.h>


namespace phxqueue_phxrpc {

namespace comm {


class FileLoader {
  public:
    FileLoader();
    FileLoader(const std::string &file);
    virtual ~FileLoader();

    void SetCheckInterval(const int check_interval_s);
    void SetRefreshDelay(const int refresh_delay_s);
    bool SetFileIfUnset(const std::string &file);

    void ResetTimeByInode(ino_t ino);
    bool LoadIfModified();
    bool LoadIfModified(bool &is_modified);
    uint64_t GetLastModTime();

    virtual bool LoadFile(const std::string &file, bool is_reload) = 0;

  private:
    class FileLoaderImpl;
    std::unique_ptr<FileLoaderImpl> impl_;
};


}  // namespace comm

}  // namespace phxqueue_phxrpc

