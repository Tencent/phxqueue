/*
Tencent is pleased to support the open source community by making PhxQueue available.
Copyright (C) 2017 THL A29 Limited, a Tencent company. All rights reserved.
Licensed under the BSD 3-Clause License (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at

<https://opensource.org/licenses/BSD-3-Clause>

Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.
*/



#pragma once

#include <algorithm>
#include <map>
#include <mutex>
#include <string>
#include <vector>


namespace phxqueue_phxrpc {

namespace mqttbroker {


template <typename Key, typename Value>
class CircularQueueData {
  public:
    inline size_t size() const {
        return in_pos - out_pos;
    }

    inline size_t ModPos(const size_t pos) const {
        return pos & (vec.size() - 1);  // pos % size
    }

    std::vector<std::pair<Key, Value>> vec;
    size_t in_pos{0u};
    size_t out_pos{0u};
    mutable std::mutex mutex;
};

template <typename Key, typename Value>
class CircularQueue {
  public:
    typedef std::pair<Key, Value> Item;

    class const_iterator : public std::iterator<std::forward_iterator_tag, Item> {
      public:
        const_iterator(const CircularQueueData<Key, Value> *const data, const size_t pos)
                : data_(data), pos_(pos) {}

        const_iterator(const const_iterator &rhs)
                : data_(rhs.data_), pos_(rhs.pos_) {}

        const_iterator(const_iterator &&rhs)
                : data_(std::move(rhs.data_)), pos_(std::move(rhs.pos_)) {}

        virtual ~const_iterator() = default;

        const_iterator &operator=(const const_iterator &rhs) {
            data_ = rhs.data_;
            pos_ = rhs.pos_;

            return *this;
        }

        const_iterator &operator=(const_iterator &&rhs) {
            data_ = std::move(rhs.data_);
            pos_ = std::move(rhs.pos_);

            return *this;
        }

        const_iterator &operator++() {
            std::lock_guard<std::mutex> guard(data_->mutex);

            ++pos_;

            return *this;
        }

        bool operator==(const_iterator other) const {
            std::lock_guard<std::mutex> guard(data_->mutex);

            return (pos_ == other.pos_);
        }

        bool operator!=(const_iterator other) const {
            std::lock_guard<std::mutex> guard(data_->mutex);

            return (pos_ != other.pos_);
        }

        Item operator*() const {
            std::lock_guard<std::mutex> guard(data_->mutex);

            if (Valid())
                return data_->vec.at(data_->ModPos(pos_));
            else
                throw std::out_of_range("invalid key");
        }

        inline bool Valid() const {
            return (data_->in_pos >= data_->out_pos) ?
                    (pos_ >= data_->out_pos && pos_ < data_->in_pos) :
                    (pos_ >= data_->out_pos || pos_ < data_->in_pos);
        }

      private:
        const CircularQueueData<Key, Value> *data_;
        size_t pos_{0u};
    };  // class const_iterator

    static inline uint32_t NextPowerOf2(const uint32_t n) {
        uint32_t tmp(n - 1);
        if (0 == tmp)
            return 1;

        tmp |= tmp >> 1;
        tmp |= tmp >> 2;
        tmp |= tmp >> 4;
        tmp |= tmp >> 8;
        tmp |= tmp >> 16;

        return ++tmp;
    }

    CircularQueue(const size_t max_size) {
        // max_size must be power of 2
        data_.vec.resize(NextPowerOf2(max_size));
    }

    virtual ~CircularQueue() = default;

    inline const_iterator cbegin() const {
        return const_iterator(&data_, data_.out_pos);
    }

    inline const_iterator cend() const {
        return const_iterator(&data_, data_.in_pos);
    }

    inline const size_t size() const {
        return data_.size();
    }

    int push_back(const Key &key, const Value &value) {
        std::lock_guard<std::mutex> guard(data_.mutex);

        data_.vec[data_.ModPos(data_.in_pos++)] = std::make_pair(key, value);
        if (data_.size() > data_.vec.size()) {
            ++data_.out_pos;
        }

        return 0;
    }

    int emplace_back(Key &&key, Value &&value) {
        std::lock_guard<std::mutex> guard(data_.mutex);

        data_.vec[data_.ModPos(data_.in_pos++)] = std::make_pair(std::move(key), std::move(value));
        if (data_.size() > data_.vec.size()) {
            ++data_.out_pos;
        }

        return 0;
    }

    int pop_front(Key &key, Value &value) {
        std::lock_guard<std::mutex> guard(data_.mutex);

        if (0 >= data_.size())
            return -1;

        key = std::move(data_.vec[data_.ModPos(data_.out_pos)].first);
        value = std::move(data_.vec[data_.ModPos(data_.out_pos)].second);
        ++data_.out_pos;

        return 0;
    }

    std::string ToString() const {
        std::lock_guard<std::mutex> guard(data_.mutex);

        std::string s("out_pos: " + std::to_string(data_.out_pos) +
                      ", in_pos: " + std::to_string(data_.in_pos) +
                      ", max_size: " + std::to_string(data_.vec.size()) +
                      ", size: " + std::to_string(data_.size()) + ", items: ");
        for (auto item : data_.vec) {
            s += "(" + std::to_string(item.first) + ", " + std::to_string(item.second) + "), ";
        }

        return s;
    }

  private:
    CircularQueueData<Key, Value> data_;
};


template <typename T>
bool GreaterThan(const T &a, const T &b) {
    return a.first > b.first;
}

template <typename Key, typename Value>
class LruCache {
  public:
    typedef std::pair<Key, Value> Item;

    LruCache(const size_t max_size) : MAX_SIZE(max_size) {}
    virtual ~LruCache() = default;

    int Get(const Key &key, Value &value) {
        std::lock_guard<std::mutex> guard(mutex_);

        auto it(map_.find(key));
        if (map_.end() == it) {
            return 1;
        }

        value = vec_.at(it->second).second;

        return 0;
    }

    int Put(const Key &key, const Value &value) {
        std::lock_guard<std::mutex> guard(mutex_);

        while (MAX_SIZE <= vec_.size()) {
            Key key;
            Value value;
            if (0 != DeleteLru(key, value)) {
                return -1;
            }
        }

        vec_.push_back(std::make_pair(key, value));

        map_[key] = vec_.size();

        std::push_heap(vec_.begin(), vec_.end(), GreaterThan<Item>);

        return 0;
    }

    std::string ToString() const {
        std::lock_guard<std::mutex> guard(mutex_);

        std::string s("max_size: " + std::to_string(MAX_SIZE) + ", size: " + std::to_string(vec_.size()) + ", items: ");
        for (auto item : vec_) {
            s += "(" + std::to_string(item.first) + ", " + std::to_string(item.second) + "), ";
        }

        return s;
    }

  private:
    int DeleteLru(Key &key, Value &value) {
        std::pop_heap(vec_.begin(), vec_.end(), GreaterThan<Item>);

        auto kv(vec_.back());
        key = kv.first;
        value = kv.second;
        auto it(map_.find(key));
        if (map_.end() != it) {
            map_.erase(it);
        }

        vec_.pop_back();

        return 0;
    }

    const size_t MAX_SIZE;

    std::vector<Item> vec_;
    std::map<Key, size_t> map_;
    mutable std::mutex mutex_;
};


}  // namespace mqttbroker

}  // namespace phxqueue_phxrpc

