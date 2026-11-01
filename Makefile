#CC=clang-9
CC=gcc
GCC=gcc

CFLAGS_COMMON = -O3 -march=native -flto
CFLAGS_WARN = -Wall -Wextra -Wcast-align -Wcast-qual -Wchar-subscripts \
	      -Wformat -Wmissing-prototypes -Wnested-externs -Wno-cast-align \
	      -Wpointer-arith -Wreturn-type -Wshadow -Wstrict-prototypes \
	      -Wswitch -Wunused-parameter -Wwrite-strings

# Library sources.  task_lib.h is the public API; task_internal.h is private.
TASK_SRC = ph.c task_core.c task_worker.c task_io.c task_timer.c
TASK_DEPS = task_lib.h task_internal.h task_atomics.h stewticket_thr.h ph.h $(TASK_SRC)

all:		echoes load_gen

load_gen:	load_gen.c $(TASK_DEPS)
		$(CC) $(CFLAGS_COMMON) $(TASK_SRC) load_gen.c -o load_gen -lpthread

load_gen-asan:	load_gen.c $(TASK_DEPS)
		$(GCC) -g3 -ggdb $(CFLAGS_WARN) -Werror -fsanitize=address -fno-omit-frame-pointer -o load_gen-asan $(TASK_SRC) load_gen.c -lpthread

load_gen-prof:	load_gen.c $(TASK_DEPS)
		$(CC) -pg -g -march=native -fprofile-arcs -ftest-coverage -o load_gen-prof $(TASK_SRC) load_gen.c -lpthread

echoes:		echoes.c $(TASK_DEPS)
		$(CC) $(CFLAGS_COMMON) $(TASK_SRC) echoes.c -o echoes -lpthread

echoes-asan:	echoes.c $(TASK_DEPS)
		$(GCC) -g3 -ggdb $(CFLAGS_WARN) -Werror -fsanitize=address -fno-omit-frame-pointer -o echoes-asan $(TASK_SRC) echoes.c -lpthread

echoes-prof:	echoes.c $(TASK_DEPS)
		$(CC) -pg -g -march=native -fprofile-arcs -ftest-coverage -o echoes-prof $(TASK_SRC) echoes.c -lpthread

# Sanitizer targets are opt-in because they need a working ASan runtime.
asan:		echoes-asan load_gen-asan

prof:		echoes-prof load_gen-prof

# Regression tests for the concurrency/lifetime fixes.
check:		$(TASK_DEPS)
		./tests/run.sh

# AddressSanitizer variant of the regression tests.  The migration/timer
# stress test and the destroy/API race test are most useful under ASan.
check-asan:	$(TASK_DEPS)
		CC="$(GCC)" EXTRA_CFLAGS="-fsanitize=address -fno-omit-frame-pointer" ./tests/run.sh

clean:
	rm -f echoes echoes-asan echoes-prof load_gen load_gen-asan load_gen-prof
	rm -f *.gcda *.gcno *.gcov *.gcov.* *.gcov-* gmon.out core
