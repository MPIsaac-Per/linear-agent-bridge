#include <errno.h>
#include <inttypes.h>
#include <limits.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>

#include <libproc.h>
#include <sys/proc_info.h>

int main(int argc, char **argv) {
  if (argc != 2) {
    return 64;
  }

  errno = 0;
  char *end = NULL;
  const long parsed_pid = strtol(argv[1], &end, 10);
  if (errno != 0 || end == argv[1] || *end != '\0' || parsed_pid <= 0 ||
      parsed_pid > INT_MAX) {
    return 64;
  }

  struct proc_bsdinfo info = {0};
  const int bytes = proc_pidinfo((int)parsed_pid, PROC_PIDTBSDINFO, 0, &info,
                                 (int)sizeof(info));
  if (bytes != (int)sizeof(info) || info.pbi_start_tvsec == 0 ||
      info.pbi_start_tvusec >= 1000000) {
    return 1;
  }

  if (printf("%" PRIu64 ":%" PRIu64 "\n", info.pbi_start_tvsec,
             info.pbi_start_tvusec) < 0) {
    return 1;
  }
  return 0;
}
