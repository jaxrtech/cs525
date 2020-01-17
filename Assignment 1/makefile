CC = gcc
CFLAGS = -Wall

default : test_assign1

HEADERS = test_helper.h storage_mgr.h dberror.h
OBJECTS = test_assign1_1.o storage_mgr.o dberror.o

#makefile cheatsheet: https://devhints.io/makefile

#generic objects compile (objects depend on *.c and *.h files)
%.o : %.c $(HEADERS)
	$(CC) $(CFLAGS) -c -o $@ $< 


test_assign1 : $(OBJECTS)
	$(CC) $(CFLAGS) $(CFLAGS) -o $@ $^ 

.PHONY : clean
clean : #rm command is not working on powershell for this version of make
	$(RM) *.exe *.o *.bin *.txt # suppress output and delete non-code files

.PHONY : run
run : 
	./test_assign1.exe