#include <stdio.h>
#include <stdint.h>
#include <string.h>

/* linux specific */
#include <unistd.h>
#include <errno.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <stdbool.h>

#include "dberror.h"
#include "storage_mgr.h"

/* manipulating page files */

void initStorageManager (void){}


/**
 * Create a new page file `fileName`.
 * The initial file size should be one page.
 * This method should fill this single page with '\0' bytes.
 *
 * @param fileName  the file path
 */
RC createPageFile (char *fileName) {

    // Atomically create file *only* if it does not exist
    // We don't want to potentially overwrite data
    int fd = open(fileName, O_CREAT | O_EXCL | O_RDWR, S_IRUSR | S_IWUSR);
    if (fd < 0) {
        switch (errno) {
            case EEXIST:
                return RC_OK;

            case EACCES:
            case EPERM:
                return RC_FILE_PERMISSIONS_ERROR;

            default:
                return RC_WRITE_FAILED;
        }
    }

    if (ftruncate(fd, PAGE_SIZE) != 0) {
        switch (errno) {
            case EACCES:
            case EPERM:
                return RC_FILE_PERMISSIONS_ERROR;

            default:
                return RC_WRITE_FAILED;
        }
    }

    return RC_OK;
}

/**
 * Opens an existing page file.
 *
 * If opening the file is successful, then the fields of this file handle will
 * be initialized with the information about the opened file.
 *
 * For instance, you would have to read the total number of pages that are
 * stored in the file from disk.
 *
 * @param fHandle  (out) file handle
 * @returns
 *   RC_OK, if successful.<br>
 *   RC_FILE_NOT_FOUND, if the file does not exist.<br>
 *   RC_FILE_PERMISSIONS_ERROR, if unable to access the file.<br>
 *   RC_FILE_HANDLE_NOT_INIT, if `fHandle` is null or internal error.<br>
 */
RC openPageFile (char *fileName, SM_FileHandle *fHandle)
{
    if (fHandle == NULL) {
        return RC_FILE_HANDLE_NOT_INIT;
    }

    // Open file for read+write
    FILE *file = NULL;
    if ((file = fopen(fileName, "r+")) == NULL) {
        int reason = errno;
        switch (reason) {
            case EPERM:
            case EACCES:
                return RC_FILE_PERMISSIONS_ERROR;
            case ENOENT:
                return RC_FILE_NOT_FOUND;
            default:
                return RC_FILE_HANDLE_NOT_INIT;
        }
    }

    // Determine file size
    struct stat file_info;
    int fd = fileno(file);
    if (fd < 0) {
        return RC_FILE_HANDLE_NOT_INIT;
    }

    if (fstat(fd, &file_info) != 0) {
        if (errno == EACCES) {
            return RC_FILE_PERMISSIONS_ERROR;
        } else {
            return RC_FILE_HANDLE_NOT_INIT;
        }
    }

    long size = file_info.st_size;
    int num_pages = (int) (size / PAGE_SIZE);

    // Initialize file handle struct
    fHandle->fileName = fileName;
    fHandle->totalNumPages = num_pages;
    fHandle->curPagePos = 0; // opening points to first file page
    fHandle->mgmtInfo = file;

    return RC_OK;
}

/**
 * Closes an open page file.
 * @param fHandle  (in) file handle
 * @return
 *      RC_OK, if success.<br>
 *      RC_FILE_HANDLE_NOT_INIT, if the file handle is null
 *      RC_WRITE_FAILED, if failed to close the file handle
 */
RC closePageFile (SM_FileHandle *fHandle)
{
    if (fHandle == NULL) {
        return RC_FILE_HANDLE_NOT_INIT;
    }

    // On POSIX, file handle is stored under `mgmtInfo`
    // Check if the file was not opened in the first place
    FILE *f = fHandle->mgmtInfo;
    if (f == NULL) {
        return RC_FILE_HANDLE_NOT_INIT;
    }

    // Attempt to close the file
    if (fclose(f) != 0) {
        return RC_WRITE_FAILED;
    }

    // Set handle within the struct to `NULL`
    fHandle->mgmtInfo = NULL;

    return RC_OK;
}

/**
 * Delete a page file.
 * @param fileName  the file path
 * @return RC_OK, if success
 */
RC destroyPageFile (char *fileName) {
	if (remove(fileName) != 0) {
		return RC_FILE_DESTROY_ERROR; 
	}

	return RC_OK;
}

/* reading blocks from disc */
	/* There are two types of read and write methods that have to be implemented: 
	     1) Methods with absolute addressing (e.g., readBlock) 
	     2) Methods that address relative to the current page of a file (e.g., readNextBlock).
*/
/**
 * Reads the `pageNum`th block from a file and stores its content in the memory
 * pointed to by the `memPage` page handle.
 * 
 * If the file has less than `pageNum` pages, returns RC_READ_NON_EXISTING_PAGE.
 * 
 * @param pageNum  the page number to read
 * @param fHandle  (in)  the file handle
 * @param memPage  (out) the page handle
 * @return 
 *      RC_OK, if successful.<br>
 *      RC_FILE_HANDLE_NOT_INIT, if the file handle is null or not opened.<br>
 *      RC_READ_NON_EXISTING_PAGE, if attempted to read page that does not 
 *          exist.
 */
RC readBlock (int pageNum, SM_FileHandle *fHandle, SM_PageHandle memPage){
	if (fHandle == NULL) {
	    return RC_FILE_HANDLE_NOT_INIT;
	}
	
	FILE *f = fHandle->mgmtInfo;
	if (f == NULL) {
	    return RC_FILE_HANDLE_NOT_INIT;
	}
	
	if (pageNum < 0 || pageNum >= fHandle->totalNumPages) {
	    return RC_READ_NON_EXISTING_PAGE;
	}

    // Seek to start of `pageNum`th page in file from the beginning of the file
    int offset = pageNum * PAGE_SIZE;
    if (fseek(f, offset, SEEK_SET) != 0) {
        return RC_FILE_SEEK_ERROR;
    }

    // Read page from file into `memPage` buffer location
    uint64_t pages_read = fread(memPage, PAGE_SIZE, 1, f);
    if (pages_read <= 0) {
        return RC_READ_NON_EXISTING_PAGE;
    }

    // Update the current page that was seeked and read
    fHandle->curPagePos = pageNum;
    return RC_OK;
}

/**
 * @param fHandle  the file handle
 * @return the current page position in a file
 */
int getBlockPos (SM_FileHandle *fHandle)
{
	if (fHandle == NULL) {
	    return -1;
	}

    return fHandle->curPagePos;
}

/**
 * @param fHandle  the file handle
 * @return the total number of blocks in the file
 */
int getTotalNumBlocks(SM_FileHandle *fHandle)
{
    if (fHandle == NULL) {
        return -1;
    }

	return fHandle->totalNumPages;
}

/**
 * Reads the first block into `memPage` buffer
 * @param fHandle  the file handle
 * @param memPage  the page memory buffer
 * @return See `readBlock()`
 */
RC readFirstBlock (SM_FileHandle *fHandle, SM_PageHandle memPage)
{
	return readBlock(0, fHandle, memPage);
}

/**
 * Read the current block within the file.
 *
 * @remark The current block position will be updated to the block that was
 * read.
 *
 * @param fHandle  the file handle
 * @param memPage  the page memory buffer
 * @return See `readBlock()`
 */
RC readPreviousBlock (SM_FileHandle *fHandle, SM_PageHandle memPage)
{
    if (fHandle == NULL) {
        return RC_FILE_HANDLE_NOT_INIT;
    }

    // `readBlock` will handle bounds checking and updating `curPagePos`
    int page_pos = fHandle->curPagePos - 1;
 	return readBlock(page_pos, fHandle, memPage);
}

/**
 * Reads the current block into `memPage` buffer
 *
 * @param fHandle  the file handle
 * @param memPage  the page memory buffer
 * @return See `readBlock()`
 */
RC readCurrentBlock (SM_FileHandle *fHandle, SM_PageHandle memPage)
{
    if (fHandle == NULL) {
        return RC_FILE_HANDLE_NOT_INIT;
    }

	int cur_block = fHandle->curPagePos;
	return readBlock(cur_block, fHandle, memPage);
}

/**
 * Reads the next block into `memPage` buffer
 *
 * @remark The current block position will be updated to the block that was
 * read.
 *
 * @param fHandle  the file handle
 * @param memPage  the page memory buffer
 * @return See `readBlock()`
 */
RC readNextBlock (SM_FileHandle *fHandle, SM_PageHandle memPage)
{
    if (fHandle == NULL) {
        return RC_FILE_HANDLE_NOT_INIT;
    }

    // `readBlock` will handle bounds checking and updating `curPagePos`
    int page_pos = fHandle->curPagePos + 1;
    return readBlock(page_pos, fHandle, memPage);
}

/**
 * Reads the last block into `memPage` buffer
 *
 * @remark The current block position will be updated to the block that was
 * read.
 *
 * @param fHandle  the file handle
 * @param memPage  the page memory buffer
 * @return See `readBlock()`
 */
RC readLastBlock (SM_FileHandle *fHandle, SM_PageHandle memPage)
{
    if (fHandle == NULL) {
        return RC_FILE_HANDLE_NOT_INIT;
    }

    // `readBlock` will handle bounds checking and updating `curPagePos`
	int last_block = fHandle->totalNumPages - 1;
	return readBlock(last_block, fHandle, memPage);
}

/**
 * Writes a page to disk using absolute page position.
 * @param pageNum  the page number to write to
 * @param fHandle  (in) the file handle
 * @param memPage  (in) the page buffer
 * @return
 *      RC_OK, if successful.<br>
 *      RC_FILE_HANDLE_NOT_INIT, if file handle is null<br>
 *      RC_WRITE_FAILED, if the page does not exist, or failed due to I/O error.
 */
RC writeBlock (int pageNum, SM_FileHandle *fHandle, SM_PageHandle memPage){
	if (fHandle == NULL) {
	    return RC_FILE_HANDLE_NOT_INIT;
	}
	
	FILE *f = fHandle->mgmtInfo;
	if (f == NULL || fileno(f) < 0) {
	    return RC_FILE_HANDLE_NOT_INIT;
	}
	
	// Ensure page number within bounds
	if (pageNum < 0 || pageNum >= fHandle->totalNumPages) {
	    return RC_WRITE_FAILED;
	}

    // Seek relative from start of file and move to start of `pageNum`-th page
    int offset = pageNum * PAGE_SIZE;
    if (fseek(f, offset, SEEK_SET) != 0) {
        return RC_FILE_SEEK_ERROR;
    }

    // Attempt to write buffer to disk
	if (fwrite(memPage, PAGE_SIZE, 1, f) == 0) {
	    return RC_WRITE_FAILED;
	}

	// Update current page position
    fHandle->curPagePos = pageNum;

	return RC_OK;
}

/**
 * Writes a block to disk using the current position.
 * @param fHandle  the file handle
 * @param memPage  the buffer to write
 * @return
 */
RC writeCurrentBlock (SM_FileHandle *fHandle, SM_PageHandle memPage)
{
    if (fHandle == NULL) {
        return RC_FILE_HANDLE_NOT_INIT;
    }

	return writeBlock(fHandle->curPagePos, fHandle, memPage);
}

/**
 * Increase the number of pages in the file by one and fill it with
 * user-specified bytes.
 *
 * @param fHandle
 * @param memPage
 * @return
 */
RC writeNewBlock (SM_FileHandle *fHandle, SM_PageHandle memPage)
{
	if (fHandle == NULL) {
	    return RC_FILE_HANDLE_NOT_INIT;
	}

	FILE *f = fHandle->mgmtInfo;
	if (f == NULL || fileno(f) < 0) {
	    return RC_FILE_HANDLE_NOT_INIT;
	}

	int numberOfPages = fHandle->totalNumPages;
    int offset = PAGE_SIZE * numberOfPages;
    fseek(f, offset, SEEK_SET);
	fwrite(memPage, 1, PAGE_SIZE, f);

	fHandle->totalNumPages++;
	return RC_OK;
}

/**
 * Increase the number of pages in the file by one.
 * The new last page should be filled with zero bytes.
 */
RC appendEmptyBlock (SM_FileHandle *fHandle){
	if (fHandle == NULL) {
	    return RC_FILE_HANDLE_NOT_INIT;
	}

	FILE *f = fHandle->mgmtInfo;
    int fd = fileno(f);
    if (f == NULL || fd < 0) {
        return RC_FILE_HANDLE_NOT_INIT;
    }

	// Calculate size increased by a page
	long size = (fHandle->totalNumPages + 1) * PAGE_SIZE;
	if (ftruncate(fd, size) != 0) {
        switch (errno) {
            case EACCES:
            case EPERM:
                return RC_FILE_PERMISSIONS_ERROR;

            default:
                return RC_WRITE_FAILED;
        }
	}

	// Increase total number of page
	fHandle->totalNumPages++;
	return RC_OK;
}

/**
 * If the file has less than `numberOfPages` pages then increase the size to
 * numberOfPages.
 *
 * @param numberOfPages  number of pages required
 * @param fHandle  the file handle
 * @return
 */
RC ensureCapacity (int numberOfPages, SM_FileHandle *fHandle){
	if (fHandle == NULL) {
	    return RC_FILE_HANDLE_NOT_INIT;
	}

	FILE *f = fHandle->mgmtInfo;
    int fd = fileno(f);
    if (f == NULL || fd < 0) {
        return RC_FILE_HANDLE_NOT_INIT;
    }

    // Determine if total number of pages changed somehow
    struct stat file_stat;
    if (fstat(fd, &file_stat) != 0) {
        if (errno == EACCES) {
            return RC_FILE_PERMISSIONS_ERROR;
        } else {
            return RC_WRITE_FAILED;
        }
    }

    long file_size = file_stat.st_size;
    if (file_size != fHandle->totalNumPages * PAGE_SIZE) {
        fHandle->totalNumPages = (int) (file_size / PAGE_SIZE);
    }

    // Check if we need to expand in the first place
	int cur_num_pages = fHandle->totalNumPages;
    bool is_page_aligned = file_size % PAGE_SIZE == 0;
	if (cur_num_pages >= numberOfPages && is_page_aligned) {
	    return RC_OK;
	}

	// Expand the file to the requested number of pages
	int new_size = numberOfPages * PAGE_SIZE;
	if (ftruncate(fd, new_size) != 0) {
        switch (errno) {
            case EACCES:
            case EPERM:
                return RC_FILE_PERMISSIONS_ERROR;

            default:
                return RC_WRITE_FAILED;
        }
	}

	// Update the total number of pages
	fHandle->totalNumPages = numberOfPages;
	return RC_OK;
}
