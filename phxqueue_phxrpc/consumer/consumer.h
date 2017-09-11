#pragma once

#include <memory>

#include "phxqueue/consumer.h"


namespace phxqueue_phxrpc {

namespace consumer {


class Consumer : public phxqueue::consumer::Consumer {
  public:
    Consumer(const phxqueue::consumer::ConsumerOption &opt);
    virtual ~Consumer();

    virtual phxqueue::comm::RetCode
    UncompressBuffer(const std::string &buffer, const int buffer_type,
                     std::string &uncompressed_buffer);
    virtual void CompressBuffer(const std::string &buffer,
                                std::string &compress_buffer, const int buffer_type);
    virtual phxqueue::comm::RetCode
    Get(const phxqueue::comm::proto::GetRequest &req, phxqueue::comm::proto::GetResponse &resp);
    virtual phxqueue::comm::RetCode
    Add(const phxqueue::comm::proto::AddRequest &req, phxqueue::comm::proto::AddResponse &resp);
    virtual phxqueue::comm::RetCode
    GetAddrScale(const phxqueue::comm::proto::GetAddrScaleRequest &req,
                 phxqueue::comm::proto::GetAddrScaleResponse &resp);
    virtual phxqueue::comm::RetCode
    GetLockInfo(const phxqueue::comm::proto::GetLockInfoRequest &req,
                phxqueue::comm::proto::GetLockInfoResponse &resp);
    virtual phxqueue::comm::RetCode
    AcquireLock(const phxqueue::comm::proto::AcquireLockRequest &req,
                phxqueue::comm::proto::AcquireLockResponse &resp);

  private:
    virtual void RestoreUserCookies(const phxqueue::comm::proto::Cookies &user_cookies) {}
};


}  // namespace consumer

}  // namespace phxqueue_phxrpc

