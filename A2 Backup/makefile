CC = gcc
CFLAGS = -Wall
RM = del

all: test_assign2_1 test_assign2_2
.PHONY : all
	
HEADERS = $(wildcard *.h)
DEPS = $(wildcard *.c)
OBJS = $(patsubst %.c, %.o, $(DEPS))

%.o : %.c $(HEADERS)
	$(CC) $(CFLAGS) -c -o $@ $< 

test_assign2_1 : $(filter-out test_assign2_2.o, $(OBJS))
	$(CC) $(CFLAGS) $^ -o $@

test_assign2_2 : $(filter-out test_assign2_1.o, $(OBJS))
	$(CC) $(CFLAGS) $^ -o $@

.PHONY : clean
clean : 
	$(RM) *.exe *.o *.bin 