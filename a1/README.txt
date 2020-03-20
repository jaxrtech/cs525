CS 525 - Assignment 1 (Spring 2020)
By: Christopher Morcom & Josh Bowden

Files Created & edited:
	readme.txt ----------- explanation of code
	storage_mgr.c -------- implemented all methods in lab
	dberror.h ------------ added error codes
	test_assign1_1.c ----- added tests to test all methods while reading/writing multiple blocks

Implementation Details:

	initStorageManager 
		- sets FILE global pagefile pointer to NULL

	createPageFile
		- creates a new pagefile <fileName> 
		- writes a single page from a buffer in memory to a file and saves/closes it

	openPageFile
		- opens a pagefile <fileName> and seeks to EOF to get filehandle parameters
		- if the file exists, then it checks for both read and write permissions before editing
			- returns RC_FILE_PERMISSIONS_ERROR if user does not have either read or write permissions
		- stores filehandle parameters into a SM_Filehandle specified by user
			- filename is given, while the number of pages is the bytes 
			  in the file divided by PAGE_SIZE (4KB).
			- we assume that 4KB is read/written at a time from the pagefile to keep the
			  totalNumPages an integer. In case a fraction is written, then the fraction
			  will be truncated since it could be an error
		- this method must be called before any read/write operations can be done or there will be a crash

	closePageFile
		- checks if the file is open. will return RC_FILE_NOT_FOUND if FILE pointer is NULL or file can't be closed
		- if the file is closed successfully, then the FILE pointer will be set to NULL and RC_OK will be returned
			- this allows us to safely check if a pagefile is open or closed before editing/removing it.

	destroyPageFile
		- checks if the FILE pointer is NULL and will close the open file before destroying it.
			- in case file cannot be removed, RC_FILE_DESTROY_ERROR will be returned. 
			  (it is probably an EACCESS error from the remove() syscall)


	getBlockPos
		- simply returns the current page position (last page read) from the fileHandle's <curPagePos> attribute.

	getTotalNumBlocks
		- simply returns the total number of writable pages from the fileHandle's <totalNumPages> attribute.

	readBlock 
		- will read a block when given an *absolute* position specified as <pageNum> (error checking comes before reading the block)
		- if openPageFile() has not been called, it returns RC_FILE_HANDLE_NOT_INIT
		- if <pageNum> is an invalid value larger than the fileHandle's <totalNumPages> 
		  or is less than zero, it returns RC_READ_NON_EXISTING_PAGE
		- if fseek() returns an error, returns RC_FILE_SEEK_ERROR (commonly due to permissions)
		- if no error occurs, then a single block will be read to a buffer specified by user
		- after reading, then the fileHandle's <curPagePos> will be set to <pageNum> to indicate the last page read.
		- returns RC_OK if no errors occur

	readFirstBlock | readPreviousBlock | readCurrentBlock | readNextBlock | readLastBlock
		- gets the appropriate *absolute* block position of the desired block via 
		  the fileHandle's <curPagePos> (or <totalNumPages> for readLastBlock)
		- calls readBlock using the above parameter as <pageNum> to read the specified block
		- returns the error from readBlock() if an error occurs


	writeBlock
		- uses fwrite() to write a block from the <memPage> buffer to the pagefile
		- updates the fileHandle's <totalNumPages> and <curPagePos> to the <pageNum> 
		  specifed by the user for writing
		- returns various codes:
			- RC_FILE_HANDLE_NOT_INIT if openPageFile() has not been called
			- RC_WRITE_FAILED if an invalid <pageNum> is specified
			- RC_FILE_PERMISSIONS_ERROR if the user does not have write permissions on the file
			- RC_FILE_SEEK_ERROR if fseek() returns an error
			- RC_OK if writing a block is successful

	writeCurrentBlock
		- calls writeBlock() where the <pageNum> is the fileHandle's <curPagePos>
		- returns the error from writeBlock() if an error occurs
	
	appendEmptyBlock
		- checks for write permissions and if openPageFile() has been called
		- seeks to the end of the pagefile and writes a block of NULL chars to memory

	writeNewBlock
		- appends an empty block and writes from a buffer to the pagefile
		- returns the error from appendEmptyBlock() writeBlock() and quits if an error occurs

	ensureCapacity
		- checks for write permissions and if openPageFile() has been called
		- seeks to the end of the file and loops to write blocks of NULL characters until 
		  the pagefile has the specified <numberOfPages>
