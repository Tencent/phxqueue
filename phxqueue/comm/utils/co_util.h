#pragma once

#include <unistd.h>

namespace phxqueue {

namespace comm {

namespace utils {

bool CoWrite(const int fd, const char *buf, const int buf_len);

bool CoRead(const int fd, char *buf, const int buf_len);


}  // namespace utils

}  // namespace comm

}  // namespace phxqueue

