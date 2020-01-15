#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include "dberror.h"
#include "storage_mgr.h"

 // NOTE: 'RC' stands for 'Return Code' (see dberror.h)

/* manipulating page files */

void initStorageManager (void){
	return;
}

RC createPageFile (char *fileName){
	return 0;
}

RC openPageFile (char *fileName, SM_FileHandle *fHandle){
	return 0;
}

RC closePageFile (SM_FileHandle *fHandle){
	return 0;
}

RC destroyPageFile (char *fileName){
	return 0;
}

/* reading blocks from disc */

RC readBlock (int pageNum, SM_FileHandle *fHandle, SM_PageHandle memPage){
	return 0;
}

int getBlockPos (SM_FileHandle *fHandle){
	return 0;
}

RC readFirstBlock (SM_FileHandle *fHandle, SM_PageHandle memPage){
	return 0;
}

RC readPreviousBlock (SM_FileHandle *fHandle, SM_PageHandle memPage){
	return 0;
}

RC readCurrentBlock (SM_FileHandle *fHandle, SM_PageHandle memPage){
	return 0;
}

RC readNextBlock (SM_FileHandle *fHandle, SM_PageHandle memPage){
	return 0;
}

RC readLastBlock (SM_FileHandle *fHandle, SM_PageHandle memPage){
	return 0;
}

/* writing blocks to a page file */

RC writeBlock (int pageNum, SM_FileHandle *fHandle, SM_PageHandle memPage){
	return 0;
}

RC writeCurrentBlock (SM_FileHandle *fHandle, SM_PageHandle memPage){
	return 0;
}

RC appendEmptyBlock (SM_FileHandle *fHandle){
	return 0;
}

RC ensureCapacity (int numberOfPages, SM_FileHandle *fHandle){
	return 0;
}
