all:		echoes echoes-asan echoes-prof load_gen load_gen-asan load_gen-prof

echoes-prof:	echoes.c task_lib.h task_lib.c picohttpparser.c picohttpparser.h ph.h ph.c
		gcc -pg -g -march=native -fprofile-arcs -ftest-coverage -o echoes-prof ph.c picohttpparser.c task_lib.c echoes.c -lpthread

load_gen-prof:	load_gen.c task_lib.h task_lib.c picohttpparser.c picohttpparser.h ph.h ph.c
		gcc -pg -g -march=native -fprofile-arcs -ftest-coverage -o load_gen-prof ph.c picohttpparser.c task_lib.c load_gen.c -lpthread

load_gen-asan:	load_gen.c task_lib.h task_lib.c picohttpparser.c picohttpparser.h ph.h ph.c
		gcc -g3 -ggdb -Wall -Wextra -Werror -Wcast-align -Wcast-qual -Wchar-subscripts -Wformat -Wmissing-prototypes -Wnested-externs -Wno-cast-align -Wpointer-arith -Wreturn-type -Wshadow -Wstrict-prototypes -Wswitch -Wunused-parameter -Wwrite-strings -fsanitize=address -fno-omit-frame-pointer -o load_gen-asan ph.c picohttpparser.c task_lib.c load_gen.c -lpthread -lpthread -static-libasan -lrt

load_gen:	load_gen.c task_lib.h task_lib.c ph.h ph.c
		gcc -O2 -ggdb -march=native -finline-functions ph.c task_lib.c load_gen.c -o load_gen -lpthread

echoes-asan:	echoes.c task_lib.h task_lib.c ph.h ph.c
		gcc -g3 -ggdb -Wall -Wextra -Werror -Wcast-align -Wcast-qual -Wchar-subscripts -Wformat -Wmissing-prototypes -Wnested-externs -Wno-cast-align -Wpointer-arith -Wreturn-type -Wshadow -Wstrict-prototypes -Wswitch -Wunused-parameter -Wwrite-strings -fsanitize=address -fno-omit-frame-pointer -o echoes-asan ph.c task_lib.c echoes.c -lpthread -static-libasan -lrt

echoes:		echoes.c task_lib.h task_lib.c ph.h ph.c
		gcc -O2 -ggdb -march=native -finline-functions ph.c task_lib.c echoes.c -o echoes -lpthread

clean:
	rm -f echoes echoes-asan echoes-prof load_gen load_gen-asan load_gen-prof *.gcda *.gcno *.gcov gmon.out core
