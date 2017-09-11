#include "phxqueue/comm/utils/co_util.h"

#include <cstring>
#include <errno.h>
#include <poll.h>
#include <signal.h>


#include "phxqueue/comm/logger.h"

//#include "co_routine.h"


namespace phxqueue {

namespace comm {

namespace utils {


bool CoWrite(const int fd, const char *buf, const int buf_len) {
    int ret = 0;
    int write_len = 0;
    int nraise = 0;
	struct pollfd pf{0};
	pf.fd = fd;
	pf.events = (POLLOUT | POLLERR | POLLHUP);
	while (1) {
		errno = 0;
		ret = write(fd, buf + write_len, buf_len - write_len);
		if (0 == ret) return false;
		if (0 > ret) {
			if (errno == EINTR || errno == EAGAIN) {
                //while (0 == (nraise = co_poll(co_get_epoll_ct(), &pf, 1, 10000))); // for co_poll not support timeout = -1 yet
                while (0 == (nraise = poll(&pf, 1, 10000))); // for co_poll not support timeout = -1 yet
                if (nraise < 0) {
                    NLErr("co_poll ret %d", nraise);
                    return false;
                }
				continue;
			}
            NLErr("write ret %d err %s", ret, strerror(errno));
            return false;
		}
        write_len += ret;
        if (write_len == buf_len) return true;
	}
    return false;
}

bool CoRead(const int fd, char *buf, const int buf_len) {
  	int ret = 0;
    int read_len = 0;
    int nraise = 0;
  	struct pollfd pf = {0};
  	pf.fd = fd;
  	pf.events = (POLLIN | POLLERR | POLLHUP);
  	while (1) {
  		//if (0 == (nraise = co_poll(co_get_epoll_ct(), &pf, 1, 10000))) continue; // for co_poll not support timeout = -1 yet
  		if (0 == (nraise = poll(&pf, 1, 10000))) continue; // for co_poll not support timeout = -1 yet
        if (nraise < 0) {
            NLErr("co_poll ret %d", nraise);
            return false;
        }
  		ret = read(fd, buf + read_len, buf_len - read_len);
  		if (0 == ret) return false;
  		if (0 > ret) {
  			if (errno == EINTR || errno == EAGAIN) continue;
            NLErr("read ret %d err %s", ret, strerror(errno));
            return false;
  		}
        read_len += ret;
        if (read_len == buf_len) return true;
  	}
    return false;
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

