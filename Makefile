#CC=clang-9
CC=gcc
GCC=gcc

CFLAGS_COMMON = -O3 -march=native
CFLAGS_WARN = -Wall -Wextra -Wcast-align -Wcast-qual -Wchar-subscripts \
	      -Wformat -Wmissing-prototypes -Wnested-externs -Wno-cast-align \
	      -Wpointer-arith -Wreturn-type -Wshadow -Wstrict-prototypes \
	      -Wswitch -Wunused-parameter -Wwrite-strings

all:		echoes echoes-asan echoes-prof load_gen load_gen-asan load_gen-prof

load_gen:	load_gen.c stewticket_thr.h task_lib.h task_lib.c task_atomics.h ph.h ph.c
		$(CC) $(CFLAGS_COMMON) ph.c task_lib.c load_gen.c -o load_gen -lpthread

load_gen-asan:	load_gen.c stewticket_thr.h task_lib.h task_lib.c task_atomics.h ph.h ph.c
		$(GCC) -g3 -ggdb $(CFLAGS_WARN) -Werror -fsanitize=address -fno-omit-frame-pointer -o load_gen-asan ph.c task_lib.c load_gen.c -lpthread -static-libasan -lrt

load_gen-prof:	load_gen.c stewticket_thr.h task_lib.h task_lib.c task_atomics.h ph.h ph.c
		$(CC) -pg -g -march=native -fprofile-arcs -ftest-coverage -o load_gen-prof ph.c task_lib.c load_gen.c -lpthread

echoes:		echoes.c stewticket_thr.h task_lib.h task_lib.c task_atomics.h ph.h ph.c
		$(CC) $(CFLAGS_COMMON) ph.c task_lib.c echoes.c -o echoes -lpthread

echoes-asan:	echoes.c stewticket_thr.h task_lib.h task_lib.c task_atomics.h ph.h ph.c
		$(GCC) -g3 -ggdb $(CFLAGS_WARN) -Werror -fsanitize=address -fno-omit-frame-pointer -o echoes-asan ph.c task_lib.c echoes.c -lpthread -static-libasan -lrt

echoes-prof:	echoes.c stewticket_thr.h task_lib.h task_lib.c task_atomics.h ph.h ph.c
		$(CC) -pg -g -march=native -fprofile-arcs -ftest-coverage -o echoes-prof ph.c task_lib.c echoes.c -lpthread

clean:
	rm -f echoes echoes-asan echoes-prof load_gen load_gen-asan load_gen-prof
	rm -f *.gcda *.gcno *.gcov *.gcov.* *.gcov-* gmon.out core
