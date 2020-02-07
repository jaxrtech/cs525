#ifndef BUFFER_MANAGER_H
#define BUFFER_MANAGER_H

// Include return codes and methods for logging errors
#include "dberror.h"

// Include bool DT
#include "dt.h"

// Replacement Strategies
typedef enum ReplacementStrategy {
	RS_FIFO = 0,
	RS_LRU = 1,
	RS_CLOCK = 2,
	RS_LFU = 3,
	RS_LRU_K = 4
} ReplacementStrategy;

// Data Types and Structures
typedef int PageNumber;
#define NO_PAGE -1

typedef struct BM_BufferPool { //Page Directory
	char *pageFile;
	int numPages;
	ReplacementStrategy strategy;
	BP_Metadata *mgmtData; // use this one to store the bookkeeping info your buffer
					// manager needs for a buffer pool
} BM_BufferPool;

typedef struct BP_Metadata //stores infor for page replacement pointed to by mgmtinfo
{
	BM_PageHandle *pageTable; //array of pointers to a list of Pages and indices (offset used in FIFO)
	int LRU_Page;	//store array OFFSET for LRU replacement
	int LFU_Page;	//store array OFFSET for LFU replacement
	int refCounter; //num of threads accessing PAGE DIR (increment before accessing)
} BP_Metadata;

typedef struct BM_PageHandle { //page dir entry
	PageNumber pageNum; //first page is zero (also page ID)
	char *data;			//points to page content in memory
	int dirtyFlag; 		//for clock replacement
} BM_PageHandle;


// convenience macros
#define MAKE_POOL()					\
		((BM_BufferPool *) malloc (sizeof(BM_BufferPool)))

#define MAKE_PAGE_HANDLE()				\
		((BM_PageHandle *) malloc (sizeof(BM_PageHandle)))

// Buffer Manager Interface Pool Handling
RC initBufferPool(BM_BufferPool *const bm, const char *const pageFileName, 
		const int numPages, ReplacementStrategy strategy,
		void *stratData);
RC shutdownBufferPool(BM_BufferPool *const bm);
RC forceFlushPool(BM_BufferPool *const bm);

// Buffer Manager Interface Access Pages
RC markDirty (BM_BufferPool *const bm, BM_PageHandle *const page);
RC unpinPage (BM_BufferPool *const bm, BM_PageHandle *const page);
RC forcePage (BM_BufferPool *const bm, BM_PageHandle *const page);
RC pinPage (BM_BufferPool *const bm, BM_PageHandle *const page, 
		const PageNumber pageNum);

// Statistics Interface
PageNumber *getFrameContents (BM_BufferPool *const bm);
bool *getDirtyFlags (BM_BufferPool *const bm);
int *getFixCounts (BM_BufferPool *const bm);
int getNumReadIO (BM_BufferPool *const bm);
int getNumWriteIO (BM_BufferPool *const bm);

#endif
