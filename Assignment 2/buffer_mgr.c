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
#include "freespace.h"

//Helper Functions
static RC evict(BM_BufferPool *bm);
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

    // allocate hash map
    meta->pageMapping = HashMap_create(128);

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
    BM_LinkedList *pageTable = bmdata->pageTable;
    BM_LinkedListElement *el = pageTable->head;
	for (uint32_t i = 0; i < bm->numPages; ++i) {
	    if (el == pageTable->sentinel) { break; }
	    BM_PageHandle *page = (BM_PageHandle *) el->data;
		if (page->dirtyFlag == 1){
			forcePage(bm, page);
			el = el->next;
		}
	}
	return RC_OK;
}

// Buffer Manager Interface Access Pages
RC markDirty (BM_BufferPool *const bm, BM_PageHandle *const page){
    BP_Metadata *meta = bm->mgmtData;
    if (page) {
        page->dirtyFlag = 1;
        meta->stats->dirtyFlags[page->pageNum] = TRUE;
		return RC_OK;
	}
	return RC_PAGE_NOT_IN_BUFFER;
}

RC unpinPage (BM_BufferPool *const bm, BM_PageHandle *const page) {
	BP_Metadata *meta = bm->mgmtData;
	if (page) {
		page->refCounter -= 1;
        meta->refCounter -= 1; //decrement buf mgr ref counter for thread use
		meta->stats->fixCounts[page->pageNum] -= 1;
		return RC_OK;
	}
	return RC_PAGE_NOT_IN_BUFFER;
}

RC forcePage (BM_BufferPool *const bm, BM_PageHandle *const page) {
    BP_Metadata *bmdata = bm->mgmtData;
	SM_FileHandle *storage = bmdata->storageManager;
	writeBlock(page->pageNum, storage, page->data);
	page->dirtyFlag = 0;
	return RC_OK;
}

RC pinPage (
        BM_BufferPool *const bm,
        BM_PageHandle *const page,
        const PageNumber pageNum)
{
	BP_Metadata *meta = bm->mgmtData;
    BM_LinkedList *pageTable = meta->pageTable;
	SM_FileHandle *storage = meta->storageManager;
	BP_Statistics *stats = meta->stats;

	// check if page number exists
    BM_LinkedListElement *el;
    void *result = NULL;
    if (HashMap_get(meta->pageMapping, pageNum, &result)) {
        el = (BM_LinkedListElement *) result;
        BM_PageHandle *handle = (BM_PageHandle *) el->data;
        handle->refCounter += 1;
        stats->fixCounts[pageNum] += 1;

    } else {
        if (meta->inUse == bm->numPages) { //evict if buffer full
            evict(bm);
        }

        // find empty space in memory pool
        el = LinkedList_fetch(pageTable);
	    if (el == NULL) {
	        fprintf(stderr, "pinPage: failed to pin page, evicted but list was full");
	        exit(1);
	    }

	    // update page number to element mapping
	    HashMap_put(meta->pageMapping, pageNum, el);

        meta->strategyHandler->insert(bm, el);
        BM_PageHandle *handle = (BM_PageHandle *) el->data;
	    readBlock(pageNum, storage, handle->data);
	}

	// fetch page from memory
	meta->refCounter += 1; //increment buf mgr ref counter for thread use
    meta->strategyHandler->use(bm, el);

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
void clearStats(BM_BufferPool *bm, uint32_t bufferPageNum) {
    BP_Metadata *meta = bm->mgmtData;
    BP_Statistics *stats = meta->stats;

    stats->frameContents[bufferPageNum] = 0;
    stats->dirtyFlags[bufferPageNum] = 0;
    stats->fixCounts[bufferPageNum] = 0;
}

RC evict(BM_BufferPool *bm){
	BP_Metadata *meta = bm->mgmtData;
    BM_LinkedList *pageTable = meta->pageTable;
    BM_LinkedListElement *el = meta->strategyHandler->elect(bm);
    if (el == NULL) {
        return RC_OK;
    }

    BM_PageHandle *page = (BM_PageHandle *) el->data;
    uint32_t pageNum = page->pageNum;
    if (page->dirtyFlag) {
        forcePage(bm, page);
    }

    clearStats(bm, page->bufferPageNum);
    page->data = NULL;
    page->pageNum = 0;
    meta->inUse -= 1;

    LinkedList_delete(pageTable, el);
    HashMap_remove(meta->pageMapping, pageNum, NULL);

	return RC_OK;
}


