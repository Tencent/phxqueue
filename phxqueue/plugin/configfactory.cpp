#include "phxqueue/plugin/configfactory.h"

#include <cassert>


namespace phxqueue {

namespace plugin {


using namespace std;


ConfigFactoryCreateFunc ConfigFactory::config_factory_create_func_ = nullptr;


}  // namespace plugin

}  // namespace phxqueue


//gzrd_Lib_CPP_Version_ID--start
#ifndef GZRD_SVN_ATTR
#define GZRD_SVN_ATTR "0"
#endif
static char gzrd_Lib_CPP_Version_ID[] __attribute__((used))="$HeadURL$ $Id$ " GZRD_SVN_ATTR "__file__";
// gzrd_Lib_CPP_Version_ID--end

