CC = gcc
CFLAGS = -Wall
RM = del

HEADERS = \
	buffer_mgr.h \
	buffer_mgr_stat.h \
	dberror.h \
	dt.h \
	storage_mgr.h \
	test_helper.h 

OBJECTS = \
	buffer_mgr_stat.c \
	storage_mgr.c \
	dberror.c \
	test_assign2_1.c \
	test_assign2_2.c

%.o : %.c $(HEADERS)
	$(CC) $(CFLAGS) -c -o $@ $< 

test_assign2_1 : $(OBJECTS)
	$(CC) $(CFLAGS) -o $@ $^ 

test_assign2_2 : $(OBJECTS)
	$(CC) $(CFLAGS) -o $@ $^ 


.PHONY : clean
clean : 
	$(RM) *.exe *.o *.bin 

.PHONY : run
