/*
Tencent is pleased to support the open source community by making PhxQueue available.
Copyright (C) 2017 THL A29 Limited, a Tencent company. All rights reserved.
Licensed under the BSD 3-Clause License (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at

<https://opensource.org/licenses/BSD-3-Clause>

Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.
*/



#pragma once

#include <type_traits>


namespace phxqueue {

namespace comm {


enum class RetCode {
    RET_OK = 0,
    RET_ERR_NO_IMPL = 10101,  // 接口未实现
    RET_ERR_NOT_MASTER = 10102,  // 本机不是master
    RET_ERR_VERSION_NOT_EQUAL = 10103,  // 乐观锁获取失败(phxlock在用)
    RET_ERR_KEY_NOT_EXIST = 10104,  // key不存在(phxlock在用)
    RET_ERR_NO_MASTER = 10105,  // 一组3机内找不到master, 一般是发生master切换但租约未过时产生
    RET_ERR_NOT_READY = 10106,
    RET_KEY_IGNORE = 10107,  // 非业务的key(phxlock在用)
    RET_ERR_SIZE_TOO_LARGE = 10108,  // 写入数据太大, 拒绝写入

    RET_ADD_SKIP = 10201,  // 写入数据被skip
    RET_DIR_NOT_EXIST = 10202,  // 目录不存在

    RET_NO_LOCK_TARGET = 10301,
    RET_ACQUIRE_LOCK_FAIL = 10302,
    RET_NO_NEED_LOCK = 10303,

    RET_NO_NEED_BLOCK = 10401,

    RET_GET_SCALE_FAIL = 10501,  // 获取权重失败
    RET_NO_NEED_UPDATE_SCALE = 10502,  // 无需更新权重
    RET_NO_LIVE_CONSUMER = 10503,  // 无存活的consumer

    RET_ERR_CURSOR_NOT_FOUND = 10601,

    RET_ERR_PAXOS_NOT_CHOSEN = 10701,

    RET_ERR_NO_NEED_DUMP_CHECKPOINT = 10801,


    RET_ERR_SYS = -1,  // 系统错误
    RET_ERR_ARG = -2,  // 参数错误
    RET_ERR_LOGIC = -104,  // 逻辑错误

    RET_ERR_UNINIT = -10001,
    RET_ERR_UNEXPECTED = -10002,  // 预料之外的错误, 一般是bug

    RET_ERR_RANGE = -10101,  // 范围不合法
    RET_ERR_RANGE_TOPIC = -10102,  // topic_id 不合法
    RET_ERR_RANGE_STORE = -10103,  // store_id 不合法
    RET_ERR_RANGE_QUEUE = -10104,  // queue_id 不合法
    RET_ERR_RANGE_CONSUMER = -10105,  // consumer IP 不合法
    RET_ERR_RANGE_HANDLE = -10106,  // handle_id 不合法
    RET_ERR_RANGE_QUEUE_INFO = -10107,  // queue_info_id 不合法
    RET_ERR_RANGE_RANK = -10108,
    RET_ERR_RANGE_CNT = -10109,
    RET_ERR_RANGE_PUB = -10110,
    RET_ERR_RANGE_CONSUMER_GROUP = -10111,
    RET_ERR_RANGE_ADDR = -10112,
    RET_ERR_RANGE_SCHEDULER = -10113,  // scheduler_id 不合法
    RET_ERR_RANGE_LOCK = -10114,  // lock_id 不合法
    RET_ERR_RANGE_VPID = -10115,  // vpid 不合法
    RET_ERR_RANGE_SUB = -10116,

    RET_ERR_NQUEUE_INVALID = -10151,  // nqueue 不合法，一般为0

    RET_ERR_GET_ADDR_FROM_CACHE = -10201,  // 访问cache失败
    RET_ERR_GET_PENDING_ADDR = -10202,  // 获取候选IP失败
    RET_ERR_KEY = -10203,
    RET_ERR_LEVELDB = -10204,  // leveldb错误
    RET_ERR_NOTIFIER_MISS = -10205,  // notifier缺失

    RET_ERR_ADD_OPEN_FAIL = -10301,
    RET_ERR_ADD_WRITE_FAIL = -10302,
    RET_ERR_GET_OPEN_FAIL_UNKNOW = -10303,
    RET_ERR_GET_ADJUST_CURSOR_ID_FAIL = -10304,  // 调整cursor失败(phxqueuestore在用)
    RET_ERR_GET_CURSOR_FAIL = -10305,  // 获取cursor失败(phxqueuestore在用)
    RET_ERR_GET_CURSOR_UNFOUND = -10306,  // cursor未找到(phxqueuestore在用)
    RET_ERR_GET_SEEK_FAIL = -10307,
    RET_ERR_GET_INVALID_BUFFER = -10308,
    RET_ERR_GET_UPDATE_CURSOR_ID_FAIL = -10309,  // 更新cursor失败(phxqueuestore在用)
    RET_ERR_CHECK_MASTER_FAIL = -10310,  // 检查master失败
    RET_ERR_MAKE_PAXOSARGS_FAIL = -10311,  // 生成paxos数据失败
    RET_ERR_PROPOSE = -10312,  // paxos propose 失败
    RET_ERR_PROCESS_PAXOSARGS = -10314,  // paxos数据参数错误
    RET_ERR_GET_TOPID_ID_AND_STORE_ID_BY_PAXOS_ADDR = -10315,  // paxos 地址非法
    RET_ERR_GET_LOCAL_IDC_STORE_CONFIG_BY_TOPIC_ID = -10316,
    RET_ERR_GET_STORE_BY_STORE_ID = -10317,  // store_id 非法
    RET_ERR_PROPOSE_TIMEOUT = -10318,  // propose 超时
    RET_ERR_PROPOSE_FAST_REJECT = -10319,  // propose fast reject
    RET_ERR_GET_ITEM_BY_CURSOR_ID = -10320,  // 根据 cursor_id 取数据失败

    RET_ERR_PROTOBUF_PARSE = -10401,  // protobuf 反序列化错误
    RET_ERR_PROTOBUF_SERIALIZE = -10402,  // protobuf 序列化错误

    RET_ERR_STORE = -10501,  // phxqueuestore 失败

    RET_ERR_SVR_BLOCK = -10601,  // 机器被屏蔽
    RET_ERR_MASTER_CLIENT_FAIL = -10602,
    RET_ERR_NO_MASTER_ADDR = -10603,

    RET_ERR_MMLB = -10701,

    RET_ERR_UNCOMPRESS = -10801,
    RET_ERR_BUFFER_TYPE = -10802,

    RET_ERR_PAXOS_RUN_NODE = -10901,
    RET_ERR_PAXOS_GET_INSTANCE_VALUE = -10902,
    RET_ERR_PAXOS_VALUE_PARSE = -10903,
};


template <typename Enumeration>
    typename std::underlying_type<Enumeration>::type as_integer(Enumeration const value) {
    return static_cast<typename std::underlying_type<Enumeration>::type>(value);
}

#define PHX_ASSERT(left,operator,right) { if(!((left) operator (right))) { std::cerr << "ASSERT FAILED: " << #left << #operator << #right << " @ " << __FILE__ << " (" << __LINE__ << "). " << #left << "=" << (left) << "; " << #right << "=" << (right) << std::endl; abort();} }

}  // namespace comm

}  // namespace phxqueue

