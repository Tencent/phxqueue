#pragma once

#include <memory>
#include <string>


namespace phxqueue_phxrpc {

namespace comm {


class FileConfig {
  public:
    FileConfig();
    FileConfig(const std::string &file);
    virtual ~FileConfig();

    void SetFile(const std::string &file);
    bool Read();
    bool GetContent(std::string &content);

  private:
    virtual void ParseContent(const std::string &content) {}

    class FileConfigImpl;
    std::unique_ptr<FileConfigImpl> impl_;
};


}  // namespace comm

}  // namespace phxqueue_phxrpc

