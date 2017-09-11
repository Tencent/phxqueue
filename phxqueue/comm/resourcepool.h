#pragma once

#include <map>
#include <queue>
#include <memory>
#include <random>
#include <sstream>
#include <mutex>

namespace phxqueue {

namespace comm {

template <typename KeyType, typename ResourceType>
class ResourcePoll {
  public:
    ResourcePoll() {}
    ~ResourcePoll() {}

    static ResourcePoll *GetInstance() {
        static ResourcePoll pool;
        return &pool;
    }

    std::unique_ptr<ResourceType> Get(const KeyType &key) {
        std::lock_guard<std::mutex> lg(lock_);

        auto &&it = key2resources_.find(key);
        if (key2resources_.end() == it) return nullptr;
        auto &resources = it->second;
        if (resources.empty()) return nullptr;
        auto resource = std::move(resources.front());
        resources.pop();
        return resource;
    }

    void Put(const KeyType &key, std::unique_ptr<ResourceType> &resource) {
        std::lock_guard<std::mutex> lg(lock_);

        auto &&resources = key2resources_[key];
        resources.push(std::move(resource));
    }

  protected:
    std::map<KeyType, std::queue<std::unique_ptr<ResourceType> > > key2resources_;
    std::mutex lock_;
};

}  // namespace comm

}  // namespace phxqueue

