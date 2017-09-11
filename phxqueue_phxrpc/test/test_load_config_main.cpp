#include <cinttypes>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <errno.h>
#include <fcntl.h>
#include <iostream>
#include <memory>
#include <netinet/in.h>
#include <poll.h>
#include <sys/socket.h>
#include <time.h>
#include <unistd.h>

#include "phxqueue_phxrpc/test/simpleconfig.h"


using namespace std;


int main() {
    phxqueue_phxrpc::test::SimpleConfig simple_config("./etc/simpleconfig.conf");

    while (1) {
        sleep(1);

        bool is_modified{false};
        simple_config.LoadIfModified(is_modified);

        if (is_modified) cout << "modified" << endl;
        cout << simple_config.GetProto().DebugString() << endl;
        cout << simple_config.GetProto().bars_size() << endl;
    }

    return 0;
}


//gzrd_Lib_CPP_Version_ID--start
#ifndef GZRD_SVN_ATTR
#define GZRD_SVN_ATTR "0"
#endif
static char gzrd_Lib_CPP_Version_ID[] __attribute__((used))="$HeadURL: http://gz-svn.tencent.com/gzrd/gzrd_mail_rep/mmcomm_proj/trunk/mmcomm/mmsection2pb/testsection2pb.cpp $ $Id: testsection2pb.cpp 1282654 2015-09-12 14:19:46Z unixliang $ " GZRD_SVN_ATTR "__file__";
// gzrd_Lib_CPP_Version_ID--end

