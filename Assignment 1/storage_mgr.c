#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include "dberror.h"
#include "storage_mgr.h"

/* NOTES */
 // NOTE: 'RC' stands for 'Return Code' (see dberror.h)
 // PAGE SIZE is constant in dberror.h as 4KB
 /* for fseek(): (ZERO ON SUCCESS)
 int fseek(FILE *pointer, long int offset, int position)
		pointer: pointer to a FILE object that identifies the stream.
		offset: number of bytes to offset from position
		position: position from where offset is added. (SEEK_END = EOF, SEEK SET = START OF FILE, SEEK_CUR = current fpointer's position)
*/
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
		fwrite(block, 1, PAGE_SIZE, file); //write to file (disk)
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
	/* There are two types of read and write methods that have to be implemented: 
	     1) Methods with absolute addressing (e.g., readBlock) 
	     2) Methods that address relative to the current page of a file (e.g., readNextBlock).
*/

RC readBlock (int pageNum, SM_FileHandle *fHandle, SM_PageHandle memPage){
	/*	The method reads the pageNum-th block from a file and stores its content in the memory pointed to by the memPage page handle. */
	/* If the file has less than pageNum pages, the method should return RC_READ_NON_EXISTING_PAGE. */
	if ((*fHandle).totalNumPages < pageNum){ return RC_READ_NON_EXISTING_PAGE+9; }
	else {
		fseek(file, pageNum*PAGE_SIZE, SEEK_SET); //seek to start of file and move to start of pageNum-th page in block
		int file_length;
		if ((file_length = fread(memPage, 1, PAGE_SIZE, file)) != PAGE_SIZE){ // read content from memPage page handle to FILE *file
			return RC_READ_NON_EXISTING_PAGE; // returns error if block not equal to PAGE_SIZE
		}
		else {
			(*fHandle).curPagePos = pageNum; //update curPagePosition on read
			return RC_OK;
		}
	}
}

int getBlockPos (SM_FileHandle *fHandle){ //return current page position in file
	return (*fHandle).curPagePos;
}

RC readFirstBlock (SM_FileHandle *fHandle, SM_PageHandle memPage){ //simply reads the first block
	RC return_code = readBlock(0,fHandle, memPage); //first block position is always relatively zero
	return return_code;
}

RC readPreviousBlock (SM_FileHandle *fHandle, SM_PageHandle memPage){ 
	int prev_block = (*fHandle).curPagePos-1; //get previous block location
	if(prev_block < 0) {return RC_READ_NON_EXISTING_PAGE;} // can't read nonexistent block
	RC return_code = readBlock(prev_block, fHandle, memPage); //try and read the block or return error above if block not pf size PAGE_SIZE
	return return_code;
}

RC readCurrentBlock (SM_FileHandle *fHandle, SM_PageHandle memPage){
	int cur_block = (*fHandle).curPagePos;
	return readBlock(cur_block, fHandle, memPage);
}

RC readNextBlock (SM_FileHandle *fHandle, SM_PageHandle memPage){
	int next_block = (*fHandle).curPagePos+1;
	return readBlock(next_block, fHandle, memPage);
}

RC readLastBlock (SM_FileHandle *fHandle, SM_PageHandle memPage){
	int last_block = (*fHandle).totalNumPages; //get position of last block
	return readBlock(last_block, fHandle, memPage);
}

/* writing blocks to a page file */

RC writeBlock (int pageNum, SM_FileHandle *fHandle, SM_PageHandle memPage){
	/* Write a page to disk using an absolute position. */
	if (pageNum < 0 || pageNum > (*fHandle).totalNumPages){ return RC_WRITE_FAILED; } //error on invalid pagenum
	if(access((*fHandle).fileName, W_OK) != -1){ //check write permissions on file
		//Using code from createPageFile()
		file = fopen((*fHandle).fileName, "r+"); //if file writable, seek and write 
		fseek(file, pageNum*PAGE_SIZE, SEEK_SET); //seek to start of file and move to start of pageNum-th page in block
		fwrite(memPage, 1, PAGE_SIZE, file); //write from memPage handle to file (disk)
		int file_length = ftell(file)+1; //get file size (EOF_point+1 - f_begin_point)
		int no_pages = file_length/PAGE_SIZE;
		(*fHandle).totalNumPages = no_pages;
		(*fHandle).curPagePos = pageNum; //update curPagePosition on read
		
		fclose(file);
		return RC_OK;

	}

	return RC_WRITE_FAILED; //could not open file
}

RC writeCurrentBlock (SM_FileHandle *fHandle, SM_PageHandle memPage){
	/* Write a page to disk using either the current position. */
	return -1;
}

RC appendEmptyBlock (SM_FileHandle *fHandle){
	/* Increase the number of pages in the file by one. The new last page should be filled with zero bytes. */
	return -1;
}

RC ensureCapacity (int numberOfPages, SM_FileHandle *fHandle){
	/* If the file has less than numberOfPages pages then increase the size to numberOfPages */
	return -1;
}
