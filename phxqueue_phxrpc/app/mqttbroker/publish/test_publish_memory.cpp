/*
Tencent is pleased to support the open source community by making PhxQueue available.
Copyright (C) 2017 THL A29 Limited, a Tencent company. All rights reserved.
Licensed under the BSD 3-Clause License (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at

<https://opensource.org/licenses/BSD-3-Clause>

Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.
*/



#include <iostream>

#include "circular_queue.hpp"


using namespace std;


int main(int argc, char **argv) {
    typedef phxqueue_phxrpc::mqttbroker::CircularQueue<int, int> TestQueue;
    typedef phxqueue_phxrpc::mqttbroker::LruCache<int, int> TestLruCache;

    {
        cout << "begin circular queue test:" << endl;

        TestQueue test_queue(4);
        test_queue.push_back(1, 10);
        cout << test_queue.ToString() << endl;
        test_queue.push_back(2, 20);
        cout << test_queue.ToString() << endl;
        test_queue.push_back(3, 30);
        cout << test_queue.ToString() << endl;
        test_queue.push_back(4, 40);
        cout << test_queue.ToString() << endl;
        test_queue.push_back(5, 50);
        cout << test_queue.ToString() << endl;
        test_queue.push_back(6, 60);
        cout << test_queue.ToString() << endl;

        int k{-1}, v{-1};
        test_queue.pop_front(k, v);
        cout << test_queue.ToString() << endl;
        test_queue.pop_front(k, v);
        cout << test_queue.ToString() << endl;
        test_queue.push_back(7, 10);
        cout << test_queue.ToString() << endl;
        test_queue.push_back(8, 20);
        cout << test_queue.ToString() << endl;
        test_queue.push_back(9, 30);
        cout << test_queue.ToString() << endl;

        cout << "end circular queue test." << endl;
    }

    {
        cout << "begin lru cache test:" << endl;

        TestLruCache test_lru_cache(4);
        cout << test_lru_cache.ToString() << endl;
        test_lru_cache.Put(1, 10);
        cout << test_lru_cache.ToString() << endl;
        test_lru_cache.Put(4, 40);
        cout << test_lru_cache.ToString() << endl;
        test_lru_cache.Put(2, 20);
        cout << test_lru_cache.ToString() << endl;
        test_lru_cache.Put(6, 60);
        cout << test_lru_cache.ToString() << endl;
        test_lru_cache.Put(3, 30);
        cout << test_lru_cache.ToString() << endl;
        test_lru_cache.Put(5, 50);
        cout << test_lru_cache.ToString() << endl;

        cout << "end lru cache test." << endl;
    }
}

