CC = gcc
CFLAGS = -Wall
RM = rm

all: test_assign3_1
.PHONY : all

HEADERS = $(wildcard *.h)
HEADERS += $(wildcard ../a2/*.h)
INCLUDE_DIRS = ../a2

SOURCES = test_assign3_1.c \
	../a1/storage_mgr.c \
	../a2/buffer_mgr_stat.c \
	../a2/dberror.c \
	../a2/buffer_mgr.c \
	../a2/linked_list.c \
	../a2/freespace.c \
	../a2/replacement_strategy.c \
	../a2/hash_map.c \
	record_mgr.c \
	rm_serializer.c \
	expr.c

OBJS = $(patsubst %.c, %.o, $(SOURCES))

%.o : %.c $(HEADERS)
	@echo "  CC $<"
	@$(CC) $(CFLAGS) -I$(INCLUDE_DIRS) -c -o $@ $<

test_assign3_1 : $(OBJS)
	@echo "  CC $<"
	@$(CC) $(CFLAGS) $^ -o $@

.PHONY : clean
clean : 
	@$(RM) -f *.o ../a2/*.o
