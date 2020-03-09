CC = gcc
CFLAGS = -Wall
RM = del

all: test_assign3_1 test_expr
.PHONY : all
	
HEADERS = $(wildcard *.h)
DEPS = $(filter-out linked_list_tests.c, $(wildcard *.c))
OBJS = $(patsubst %.c, %.o, $(DEPS))

%.o : %.c $(HEADERS)
	$(CC) $(CFLAGS) -c -o $@ $< 

test_assign3_1 : $(filter-out test_expr.o, $(OBJS))
	$(CC) $(CFLAGS) $^ -o $@

test_expr : $(filter-out test_assign3_1.o, $(OBJS))
	$(CC) $(CFLAGS) $^ -o $@

.PHONY : clean
clean : 
	$(RM) *.exe *.o *.bin 