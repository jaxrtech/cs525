#ifndef BUFFER_MANAGER_H
#define BUFFER_MANAGER_H

#include <stdint.h>

// Include return codes and methods for logging errors
#include "dberror.h"

// Include bool DT
#include "dt.h"

// Include storage manager types
#include "storage_mgr.h"
#include "freespace.h"
#include "linked_list.h"
#include "hash_map.h"

#define BM_REPLACEMENT_STRAT_COUNT (5)

struct RS_StrategyHandler;

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

typedef struct BM_BufferPool {
	const char *pageFile;
	int numPages;
	ReplacementStrategy strategy;
	void *stratData;
	void *mgmtData; // use this one to store the bookkeeping info your buffer
	                // manager needs for a buffer pool
} BM_BufferPool;

typedef struct BM_PageHandle {
	PageNumber pageNum;
	char *buffer;
} BM_PageHandle;

typedef struct BP_PageDescriptor {
    BM_PageHandle handle;
    int fixCount;
    bool dirty;
} BP_PageDescriptor;

#define BM_DEREF_ELEMENT(_EL) ((BP_PageDescriptor *) (_EL)->data)

typedef struct BP_Statistics {
    int diskReads;
    int diskWrites;

    PageNumber *lastFrameContents;
    bool *lastDirtyFlags;
    int *lastFixCounts;
} BP_Statistics;

typedef struct BP_Metadata //stores infor for page replacement pointed to by mgmtinfo
{
    SM_FileHandle *storageManager;
    BM_LinkedList *pageDescriptors; // linked list of pages
    HS_HashMap *pageMapping;  // hash map of page number to page handles
    struct RS_StrategyHandler *strategyHandler;  // use forward declaration
    char *pageBuffer;            // contiguous memory pool for blocks
    int clockCount;			  // stores current position of clock in buffer table
    int refCounter;			  // no. threads accessing PAGE DIR (increment before accessing)
    int inUse;
    BP_Statistics *stats;
    void *strategyMetadata;
} BP_Metadata;

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
