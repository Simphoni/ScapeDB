#include <cstdio>
#include <cstdlib>
#include <filesystem>

#include <fcntl.h>
#include <unistd.h>

#include <storage/fastio.h>

namespace fastIO {

namespace fs = std::filesystem;

static char buf[1 << 19]; // 0.5M
static int fd = -1;
static char *cur, *tail;
static bool ended;

bool set_file(const std::string &s) {
  if (!fs::is_regular_file(s)) {
    return false;
  }
  ended = false;
  if (fd != -1) {
    close(fd);
  }
  fd = open(s.data(), O_RDONLY);
  cur = tail = buf;
  return true;
}

void end_read() {
  if (fd != -1) {
    close(fd);
    fd = -1;
  }
}

void read_buf() { tail = &buf[0] + read(fd, buf, 1 << 19); }

char getchar() {
  if (ended) {
    return -1;
  }
  if (cur == tail) {
    cur = buf;
    read_buf();
  }
  if (cur == tail) {
    ended = true;
    return -1;
  } else {
    return *cur++;
  }
}

}; // namespace fastIO