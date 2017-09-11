#pragma once

#include <cstdint>
#include <cstdio>
#include <memory>

namespace phxqueue {

namespace comm {

namespace utils {

uint64_t MurmurHash64(const void *key, size_t len, uint64_t seed);

}  // namespace utils

}  // namespace comm

}  // namespace phxqueue


