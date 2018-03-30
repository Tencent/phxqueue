/*
Tencent is pleased to support the open source community by making PhxQueue available.
Copyright (C) 2017 THL A29 Limited, a Tencent company. All rights reserved.
Licensed under the BSD 3-Clause License (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at

<https://opensource.org/licenses/BSD-3-Clause>

Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.
*/


#include "phxqueue/comm/utils/consumer_group_util.h"


namespace phxqueue {

namespace comm {

namespace utils {


using namespace std;

CompareResult CursorIDCompare(const uint64_t x, const uint64_t y) {
	uint64_t MIN_NUMBER = -1;
	if (x == MIN_NUMBER && y == MIN_NUMBER) {
		return CompareResult::EQUEL;
	}
	else if (x == MIN_NUMBER) {
		return CompareResult::SMALLER;
	}
	else if (y == MIN_NUMBER) {
		return CompareResult::BIGGER;
	}
	else {
		if (x == y) {
			return CompareResult::EQUEL;
		}
		else if (x < y) {
			return CompareResult::SMALLER;
		}
		else {
			return CompareResult::BIGGER;
		}
	}

	return CompareResult::EQUEL;
}

uint64_t ConsumerGroupIDs2Mask(const std::set<int> *consumer_group_ids) {
	uint64_t mask = -1;
	if (consumer_group_ids && !consumer_group_ids->empty()) {
		mask = 0;
		int consumer_group_id;
		for (auto &&it : *consumer_group_ids) {
			consumer_group_id = it;
			if (consumer_group_id > 0) mask |= (1ULL << (consumer_group_id - 1ULL));
		}
	}
	return mask;
}


}  // namespace utils

}  // namespace comm

}  // namespace phxqueue



//gzrd_Lib_CPP_Version_ID--start
#ifndef GZRD_SVN_ATTR
#define GZRD_SVN_ATTR "0"
#endif
static char gzrd_Lib_CPP_Version_ID[] __attribute__((used))="$HeadURL$ $Id$ " GZRD_SVN_ATTR "__file__";
// gzrd_Lib_CPP_Version_ID--end

