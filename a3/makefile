CC = gcc
CFLAGS = -g #Wall
RM = rm

all: test_assign3_1 test_expr test_binfmt
.PHONY : all
	
HEADERS = $(wildcard *.h)
DEPS_CORE = storage_mgr.c \
       buffer_mgr_stat.c \
       dberror.c \
       buffer_mgr.c \
       linked_list.c \
       freespace.c \
       replacement_strategy.c \
       hash_map.c \
       record_mgr.c \
       rm_serializer.c \
       expr.c \
       rm_page.c \
       binfmt.c

DEPS_TEST_ASSIGN3_1 = $(DEPS_CORE) test_assign3_1.c
DEPS_TEST_EXPR = $(DEPS_CORE) test_expr.c
DEPS_TEST_BINFMT = $(DEPS_CORE) binfmt_test.c

OBJS_TEST_ASSIGN3_1 = $(patsubst %.c, %.o, $(DEPS_TEST_ASSIGN3_1))
OBJS_TEST_EXPR = $(patsubst %.c, %.o, $(DEPS_TEST_EXPR))
OBJS_TEST_BINFMT = $(patsubst %.c, %.o, $(DEPS_TEST_BINFMT))

%.o : %.c $(HEADERS)
	$(CC) $(CFLAGS) -c -o $@ $< 

test_assign3_1 : $(OBJS_TEST_ASSIGN3_1)
	$(CC) $(CFLAGS) $^ -o $@

test_expr : $(OBJS_TEST_EXPR)
	$(CC) $(CFLAGS) $^ -o $@

test_binfmt : $(OBJS_TEST_BINFMT)
	$(CC) $(CFLAGS) $^ -o $@

.PHONY : clean
clean : 
	$(RM) -f *.exe *.o *.bin *.db