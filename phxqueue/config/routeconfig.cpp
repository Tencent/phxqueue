/*
Tencent is pleased to support the open source community by making PhxQueue available.
Copyright (C) 2017 THL A29 Limited, a Tencent company. All rights reserved.
Licensed under the BSD 3-Clause License (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at

<https://opensource.org/licenses/BSD-3-Clause>

Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.
*/



#include "phxqueue/config/routeconfig.h"

#include "phxqueue/comm.h"


namespace phxqueue {

namespace config {

using namespace std;

class RouteConfig::RouteConfigImpl {
	public:
		RouteConfigImpl() {};
		virtual ~RouteConfigImpl() {};

		proto::RouteGeneral route_general;
		comm::utils::JumpConsistenHash<uint64_t, std::shared_ptr<proto::Route>> jumphash;
};

RouteConfig::RouteConfig() : impl_(new RouteConfigImpl()) {
	assert(impl_);
}

RouteConfig::~RouteConfig() {}

comm::RetCode RouteConfig::ReadConfig(proto::RouteConfig &proto) {
    // sample
	proto::Route *route = nullptr;
    comm::proto::Addr *addr = nullptr;

    route = proto.add_routes();
    route->set_scale(100);
    addr = route->mutable_addr();
    addr->set_ip("127.0.0.1");
    addr->set_port(8001);

    route = proto.add_routes();
    route->set_scale(100);
    addr = route->mutable_addr();
    addr->set_ip("127.0.0.1");
    addr->set_port(8002);

    route = proto.add_routes();
    route->set_scale(100);
    addr = route->mutable_addr();
    addr->set_ip("127.0.0.1");
    addr->set_port(8003);

    return comm::RetCode::RET_OK;
}

comm::RetCode RouteConfig::Rebuild() {
    bool need_check = NeedCheck();

	impl_->jumphash.Init(
			[](const uint64_t &key)->uint64_t {
				return key;
			},
			[](const std::shared_ptr<proto::Route> &node, int scale)->uint64_t {
				string ip = node->addr().ip();
				int64_t port = node->addr().port();
				uint64_t h = 0;
				h = comm::utils::MurmurHash64(&port, sizeof(int64_t), h);
				h = comm::utils::MurmurHash64(ip.c_str(), strlen(ip.c_str()), h);
				h = comm::utils::MurmurHash64(&scale, sizeof(int), h);
				return h;
			});

	auto &&proto = GetProto();

	impl_->route_general.CopyFrom(proto.general());
	for (int i{0}; i < proto.routes_size(); ++i) {
		const auto &route(proto.routes(i));
		impl_->jumphash.AddNode(make_shared<proto::Route>(route), route.scale());
	}
	return comm::RetCode::RET_OK;
}

comm::RetCode RouteConfig::GetAllRoute(std::vector<std::shared_ptr<const proto::Route>> &routes) const {
	std::list<std::shared_ptr<proto::Route>> node_list;
	impl_->jumphash.GetAllNode(node_list);
	for (auto iter = node_list.begin(); iter != node_list.end(); iter++) {
		routes.push_back(*iter);
	}
	return comm::RetCode::RET_OK;
}

comm::RetCode RouteConfig::GetAddrByConsistentHash(const uint64_t key, comm::proto::Addr &addr) const {
	std::shared_ptr<proto::Route> route;
	bool ret = impl_->jumphash.PickNodeByKey(key, route);
	if (ret == false) {
		return comm::RetCode::RET_ERR_NO_ROUTE_ADDR;
	}
	addr = route->addr();
	return comm::RetCode::RET_OK;
}

int RouteConfig :: GetConnTimeoutMs()
{
	return impl_->route_general.conn_timeout_ms();
}


}  // namespace config

}  // namespace phxqueue



//gzrd_Lib_CPP_Version_ID--start
#ifndef GZRD_SVN_ATTR
#define GZRD_SVN_ATTR "0"
#endif
static char gzrd_Lib_CPP_Version_ID[] __attribute__((used))="$HeadURL$ $Id$ " GZRD_SVN_ATTR "__file__";
// gzrd_Lib_CPP_Version_ID--end

