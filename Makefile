all:		echoes echoes-asan echoes-prof load_gen load_gen-asan load_gen-prof

load_gen:	load_gen.c ticketlock.h task_lib.h task_lib.c ph.h ph.c
		gcc -O3 -march=native ph.c task_lib.c load_gen.c -o load_gen -lpthread

load_gen-asan:	load_gen.c ticketlock.h task_lib.h task_lib.c ph.h ph.c
		gcc -g3 -ggdb -Wall -Wextra -Werror -Wcast-align -Wcast-qual -Wchar-subscripts -Wformat -Wmissing-prototypes -Wnested-externs -Wno-cast-align -Wpointer-arith -Wreturn-type -Wshadow -Wstrict-prototypes -Wswitch -Wunused-parameter -Wwrite-strings -fsanitize=address -fno-omit-frame-pointer -o load_gen-asan ph.c picohttpparser.c task_lib.c load_gen.c -lpthread -lpthread -static-libasan -lrt

load_gen-prof:	load_gen.c ticketlock.h task_lib.h task_lib.c ph.h ph.c
		gcc -pg -g -march=native -fprofile-arcs -ftest-coverage -o load_gen-prof ph.c picohttpparser.c task_lib.c load_gen.c -lpthread

echoes:		echoes.c ticketlock.h task_lib.h task_lib.c ph.h ph.c
		gcc -O3 -march=native ph.c task_lib.c echoes.c -o echoes -lpthread

echoes-asan:	echoes.c ticketlock.h task_lib.h task_lib.c ph.h ph.c
		gcc -g3 -ggdb -Wall -Wextra -Werror -Wcast-align -Wcast-qual -Wchar-subscripts -Wformat -Wmissing-prototypes -Wnested-externs -Wno-cast-align -Wpointer-arith -Wreturn-type -Wshadow -Wstrict-prototypes -Wswitch -Wunused-parameter -Wwrite-strings -fsanitize=address -fno-omit-frame-pointer -o echoes-asan ph.c task_lib.c echoes.c -lpthread -static-libasan -lrt

echoes-prof:	echoes.c ticketlock.h task_lib.h task_lib.c ph.h ph.c
		gcc -pg -g -march=native -fprofile-arcs -ftest-coverage -o echoes-prof ph.c picohttpparser.c task_lib.c echoes.c -lpthread

clean:
	rm -f echoes echoes-asan echoes-prof load_gen load_gen-asan load_gen-prof *.gcda *.gcno *.gcov gmon.out core
