#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include "dberror.h"
#include "storage_mgr.h"

 // NOTE: 'RC' stands for 'Return Code' (see dberror.h)
 //PAGE SIZE is constant in dberror.h as 4KB

FILE *file; //global file pointer

/* manipulating page files */

void initStorageManager (void){ // does nothing for now
	return;
}

RC createPageFile (char *fileName){
	/* Create a new page file fileName. The initial file size should be one page. 
	   This method should fill this single page with '\0' bytes. 
	 */
	if ((file = fopen(fileName, "w+"))){ //create/overwrite file for read+write and check if not exist
		char *block = malloc(PAGE_SIZE); // create a 4KB block
		memset(block, '\0', PAGE_SIZE);	//fill block with '\0' bytes
		fwrite(block, sizeof(char), PAGE_SIZE, file); //write to file (disk)
		free(block);
		fclose(file);
		return RC_OK; //code 0
	} else {
		return RC_WRITE_FAILED; //error 3: cannot create new file
	}
}

RC openPageFile (char *fileName, SM_FileHandle *fHandle){
	/* Opens an existing page file. Should return RC_FILE_NOT_FOUND if the file does not exist. 
	   The second parameter is an existing file handle. If opening the file is successful, 
	   then the fields of this file handle should be initialized with the information about the opened file. 
	   For instance, you would have to read the total number of pages that are stored in the file from disk.
	 */
	if(access(fileName, R_OK|W_OK) != -1){ //access() in unistd.h for checking file exists with read+write permissions
		file = fopen(fileName, "r+"); //open file for read+write
		/*calc fHandle struct attributes*/
		fseek(file, 0L, SEEK_END); //point to EOF
		int file_length = ftell(file)+1; //get file size (EOF_point+1 - f_begin_point)
		int no_pages = file_length/PAGE_SIZE;
		rewind(file); //point back to file beginning

		/*set fHandle struct attributes*/
		(*fHandle).fileName = fileName;
		(*fHandle).totalNumPages = no_pages;
		(*fHandle).curPagePos = 0; //opening points to first file page
		//(*fHandle).mgmtInfo = NULL;
		return RC_OK; //code 0

	} else {
		return RC_FILE_NOT_FOUND; //error 1

	}
}

RC closePageFile (SM_FileHandle *fHandle){ //self-explanatory
	if(fclose(file) == 0){
		return RC_OK;
	} else {
		return RC_FILE_NOT_FOUND; //error 1
	}

}

RC destroyPageFile (char *fileName){ //delete a page file
	if(remove(fileName) == 0){ //remove() returns 0 if deleted successfully
		return RC_OK;
	} else {
		return RC_FILE_NOT_FOUND; //error 1
	}
}

/* reading blocks from disc */

RC readBlock (int pageNum, SM_FileHandle *fHandle, SM_PageHandle memPage){
	RC return_code=0;
	return return_code;
}

int getBlockPos (SM_FileHandle *fHandle){
	RC return_code=0;
	return return_code;
}

RC readFirstBlock (SM_FileHandle *fHandle, SM_PageHandle memPage){
	RC return_code=0;
	return return_code;
}

RC readPreviousBlock (SM_FileHandle *fHandle, SM_PageHandle memPage){
	RC return_code=0;
	return return_code;
}

RC readCurrentBlock (SM_FileHandle *fHandle, SM_PageHandle memPage){
	RC return_code=0;
	return return_code;
}

RC readNextBlock (SM_FileHandle *fHandle, SM_PageHandle memPage){
	RC return_code=0;
	return return_code;
}

RC readLastBlock (SM_FileHandle *fHandle, SM_PageHandle memPage){
	RC return_code=0;
	return return_code;
}

/* writing blocks to a page file */

RC writeBlock (int pageNum, SM_FileHandle *fHandle, SM_PageHandle memPage){
	RC return_code=0;
	return return_code;
}

RC writeCurrentBlock (SM_FileHandle *fHandle, SM_PageHandle memPage){
	RC return_code=0;
	return return_code;
}

RC appendEmptyBlock (SM_FileHandle *fHandle){
	RC return_code=0;
	return return_code;
}

RC ensureCapacity (int numberOfPages, SM_FileHandle *fHandle){
	RC return_code=0;
	return return_code;
}
