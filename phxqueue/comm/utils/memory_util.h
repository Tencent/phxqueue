#include <memory>


namespace phxqueue {

namespace comm {

namespace utils {


template <typename T, typename... Ts>
std::unique_ptr<T> make_unique(Ts&&... params) {
    return std::unique_ptr<T>(new T(std::forward<T>(params)...));
}


class MemStat {
  public:
    MemStat() = default;
    virtual ~MemStat() = default;


    unsigned long size{0};  // total program size
    unsigned long resident{0};  // resident set size
    unsigned long share{0};  // shared pages
    unsigned long text{0};  // text (code)
    unsigned long lib{0};  // library
    unsigned long data{0};  // data/stack
    unsigned long dt{0};  // dirty pages

    bool Stat(pid_t pid = 0);
};


}  // namespace utils

}  // namespace comm

}  // namespace phxqueue

