#include <stdio.h>
#include <stdint.h>
#include <string.h>
#include <stdlib.h>

/* linux specific */
#include <unistd.h>
#include <errno.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <stdbool.h>

#include "dberror.h"
#include "storage_mgr.h"
#include "buffer_mgr.h"
#include "replacement_strategy.h"

//Helper Functions
static BM_PageHandle *checkPool(BM_BufferPool *const bm, BM_PageHandle *page);
static RC evict(BM_BufferPool *const bm);
static void updatePageTable(BM_BufferPool *const bm, BM_PageHandle *pageTable); //call this on pin/unpin
static RC freeBufferPage(BM_BufferPool *const bm, int blockNum);
static int markNextFreeBufferPage(BM_BufferPool *const bm);
static void freeBufferPageTable(const BM_BufferPool *bm);

//

RC initBufferPool(
        BM_BufferPool *const bm,
        const char *const pageFileName,
		const int numPages,
		ReplacementStrategy strategy,
		void *stratData)
{
	printf("ASSIGNMENT 2 (Buffer Manager)\n\tCS 525 - SPRING 2020\n\tCHRISTOPHER MORCOM & JOSH BOWDEN\n\n");

	BP_Metadata *meta = NULL;
	BP_Statistics *stats = NULL;
    SM_FileHandle *storageHandle = NULL;

    //Store BM_BufferPool Attributes
    bm->pageFile = pageFileName;
    bm->numPages = numPages;
    bm->strategy = strategy;
    bm->stratData = stratData;

    //set up bookkeeping data
    meta = malloc(sizeof(BP_Metadata));
    bm->mgmtData = meta;
    meta->strategyHandler = &RS_StrategyHandlerImpl[strategy];
    meta->clockCount = 0; //for clock replacement
    meta->refCounter = 0; //nothing using buffer yet
    meta->inUse = 0;	  //no pages in use

    stats = malloc(sizeof(BP_Statistics));
    stats->diskReads = 0;
    stats->diskWrites = 0;
    stats->frameContents = calloc(numPages, sizeof(PageNumber));
    stats->dirtyFlags = calloc(numPages, sizeof(bool));
    stats->fixCounts = calloc(numPages, sizeof(int));
    meta->stats = stats;

    // allocate memory pool
    meta->blocks = calloc(numPages, PAGE_SIZE);
    meta->freespace = Freespace_create(numPages);

    //open the storage manager
    storageHandle = malloc(sizeof(SM_FileHandle));
    RC result;
    if ((result = openPageFile(pageFileName, storageHandle)) != RC_OK) {
        free(storageHandle);
        free(meta);
        return result;
    }
    meta->storageManager = storageHandle;

    //set up pagetable
    meta->pageTable = LinkedList_create(numPages, sizeof(BM_PageHandle));

	return RC_OK;
}

RC shutdownBufferPool(BM_BufferPool *const bm){
    forceFlushPool(bm); //write all dirty pages to disk
	BP_Metadata *bmdata = bm->mgmtData;
	if (bmdata->refCounter > 0){
		return RC_BM_IN_USE; //cannot free bm because a page is still in use
	}
	//free doubly linked list
    freeBufferPageTable(bm);
    free(bm->mgmtData); //free struct holding pg table
    free(bm); //free buffer manager
	return RC_OK;
}

static void freeBufferPageTable(const BM_BufferPool *bm) {
    BP_Metadata *bmdata = bm->mgmtData;
    BM_PageHandle *node = bmdata->pageTable;
    for (int i = 0; i < bm->numPages; ++i){
    	BM_PageHandle *newnode = node->next;
    	free(node);
    	node = newnode;
    }
}

RC forceFlushPool(BM_BufferPool *const bm){
	BP_Metadata *bmdata = bm->mgmtData;
	BM_PageHandle *start = (BM_PageHandle *)&bmdata->pageTable->elementsDataBuffer;
	BM_PageHandle *end = start + bm->numPages;

	for (BM_PageHandle *node = start; node < end; ++node) {
		if (node->dirtyFlag == 1) {
			forcePage(bm, node);
		}
	}

	return RC_OK;
}

// Buffer Manager Interface Access Pages
RC markDirty (BM_BufferPool *const bm, BM_PageHandle *const page){
	BM_PageHandle *pg = checkPool(bm, page);
    BP_Metadata *meta = bm->mgmtData;
    if (pg) {
        pg->dirtyFlag = 1;
        meta->stats->dirtyFlags[pg->pageNum] = TRUE;
		return RC_OK;
	}
	return RC_PAGE_NOT_IN_BUFFER;
}

RC unpinPage (BM_BufferPool *const bm, BM_PageHandle *const page){
	BM_PageHandle *pg = checkPool(bm, page);
	BP_Metadata *meta = bm->mgmtData;
	if(pg){
		pg->refCounter -= 1;
		updatePageTable(bm, meta->pageTable);
        meta->refCounter -= 1; //decrement buf mgr ref counter for thread use
		meta->stats->fixCounts[pg->pageNum] -= 1;
		return RC_OK;
	}
	return RC_PAGE_NOT_IN_BUFFER;
}

RC forcePage (BM_BufferPool *const bm, BM_PageHandle *const page){
    BP_Metadata *bmdata = bm->mgmtData;
	SM_FileHandle *storage = bmdata->storageManager;
	writeBlock(page->pageNum, storage, page->data);
	page->dirtyFlag = 0;
	return RC_OK;
}

RC pinPage (BM_BufferPool *const bm, BM_PageHandle *const page, const PageNumber pageNum){
	BP_Metadata *meta = bm->mgmtData;
	BM_PageHandle *table = meta->pageTable;
	SM_FileHandle *storage = meta->storageManager;
	BP_Statistics *stats = meta->stats;

	BM_PageHandle *pg = checkPool(bm, page);
	if (pg) { //if page in buffer, increment pin count
		pg->refCounter += 1;
        meta->stats->fixCounts[pg->pageNum] += 1;
    }
	else {
	    if (meta->inUse == bm->numPages) { //evict if buffer full
	        evict(bm);
	    }

	    // find empty space in memory pool
	    int blockNum = markNextFreeBufferPage(bm);
	    if (blockNum < 0) {
	        return RC_BM_IN_USE;
	    }

	    // read into memory from disk
	    pg = table->prev;
	    if (pg->data != NULL) {
            fprintf(stderr, "pinPage: expected free entry after evict but full\n");
	        return RC_IM_NO_MORE_ENTRIES;
	    }

	    // put at the front


	    pg->bufferPageNum = blockNum;
	    readBlock(pageNum, storage, meta->blocks[blockNum]);
	}

	// fetch page from memory
	meta->refCounter += 1; //increment buf mgr ref counter for thread use
	updatePageTable(bm, meta->pageTable);
	return RC_OK;
}


// Statistics Interface
PageNumber *getFrameContents (BM_BufferPool *const bm){
    BP_Metadata *meta = bm->mgmtData;
    BP_Statistics *stats = meta->stats;
	return stats->frameContents;
}

bool *getDirtyFlags (BM_BufferPool *const bm){
    BP_Metadata *meta = bm->mgmtData;
    BP_Statistics *stats = meta->stats;
    return stats->dirtyFlags;
}

int *getFixCounts (BM_BufferPool *const bm){
    BP_Metadata *meta = bm->mgmtData;
    BP_Statistics *stats = meta->stats;
    return stats->fixCounts;
}

int getNumReadIO (BM_BufferPool *const bm){
    BP_Metadata *meta = bm->mgmtData;
    BP_Statistics *stats = meta->stats;
    return stats->diskReads;
}

int getNumWriteIO (BM_BufferPool *const bm){
    BP_Metadata *meta = bm->mgmtData;
    BP_Statistics *stats = meta->stats;
    return stats->diskWrites;
}


/*		HELPER FUNCTIONS		*/
BM_PageHandle *checkPool(BM_BufferPool *const bm, BM_PageHandle *page){
	BP_Metadata *bmdata = bm->mgmtData;
	// TODO
	return page;
}

RC evict(BM_BufferPool *const bm){
	BP_Metadata *bmdata = bm->mgmtData;
	BM_PageHandle *toevict = bmdata->pageTable;
	int i;
	if (bm->strategy == RS_CLOCK){ //clock strategy evicts starting on counter pointer
		for (i = 0; i < bmdata->clockCount; ++i){
			toevict = toevict->next;
		}
	}
	if(&toevict == &bmdata->pageTable){ //evicting head
		bmdata->pageTable = toevict->next;
	} else {	
		for (i = 0; i < bmdata->inUse; ++i){
			if(toevict->refCounter > 0){ //if page being used
				toevict = toevict->next;
				continue;
			} 
			if(toevict->dirtyFlag > 0){ //page not used but dirty
				forcePage(bm, toevict); //write back to disk
			}
			//page not dirty or already written to disk
			toevict->prev->next = toevict->next; //unlink from ptable
			toevict->next->prev = toevict->prev;

			bmdata->pageTable->prev->next = toevict; //relink to end of ptable
			toevict->prev = bmdata->pageTable->prev; 
			bmdata->pageTable->prev = toevict;
			toevict->next = bmdata->pageTable;
		}
	}

	//clear toevict
	freeBufferPage(bm, toevict->bufferPageNum);
	toevict->data = NULL;
	toevict->dirtyFlag = 0;
	toevict->pageNum = 0;
	bmdata->inUse -= 1;
	return RC_OK;
}

void updatePageTable(BM_BufferPool *const bm, BM_PageHandle* pageTable){
	//sort linkedlist by strategy params
		//for FIFO: sort by first created (easiest)
		//for LRU: sort by last used
		//for LFU: sort by refCounter
		//for CLOCK: keep in order of FIFO, but use clock counter to choose which page to start eviction at
		//for LRU-K: ???????????????

	switch(bm->strategy){
		case 0: //RS_FIFO
			//structure natively adds/removes pages using FIFO
			break;
		case 2: //RS_CLOCK
			//CLOCK works with any order
			break;
		case 1: //RS_LRU (need to sort on add/edit/evict)
			break;
		case 3: //RS_LFU ()
			break;
		case 4: //RS_LRU_K
			break;
		default:
			break;
	}
	return;
}


