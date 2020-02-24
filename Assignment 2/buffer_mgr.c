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
#include <inttypes.h>

#include "dberror.h"
#include "storage_mgr.h"
#include "buffer_mgr.h"
#include "replacement_strategy.h"
#include "freespace.h"

//Helper Functions
static RC evict(BM_BufferPool *bm);
static bool resolveByHandle(
        BM_BufferPool *bm,
        BM_PageHandle *handle,
        BM_LinkedListElement **el_out);

static bool resolveByPageNum(
        BM_BufferPool *bm,
        PageNumber num,
        BM_LinkedListElement **el_out);

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

    // set up pagetable
    meta->pageTable = LinkedList_create(numPages, sizeof(BM_PageHandle));

    // allocate memory pool
    meta->pageBuffer = calloc(numPages, PAGE_SIZE);
    meta->freespace = Freespace_create(numPages);
    for (uint32_t i = 0; i < numPages; i++) {
        BM_LinkedListElement *el = &meta->pageTable->elementsMetaBuffer[i];
        BM_PageHandle *handle = (BM_PageHandle *) el->data;
        handle->buffer = meta->pageBuffer + (i * PAGE_SIZE);
    }

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

	return RC_OK;
}

RC shutdownBufferPool(BM_BufferPool *const bm){
    forceFlushPool(bm); //write all dirty pages to disk
//	BP_Metadata *bmdata = bm->mgmtData;
//	if (bmdata->refCounter > 0){
//		return RC_BM_IN_USE; //cannot free bm because a page is still in use
//	}
//    free(bm->mgmtData); //free struct holding pg table
//    free(bm); //free buffer manager
	return RC_OK;
}

RC forceFlushPool(BM_BufferPool *const bm){
	BP_Metadata *meta = bm->mgmtData;
	BP_Statistics *stats = meta->stats;
    BM_LinkedList *pageTable = meta->pageTable;
    BM_LinkedListElement *el = pageTable->head;
	for (uint32_t i = 0; i < bm->numPages; ++i) {
	    if (el == pageTable->sentinel) { break; }
	    BM_PageHandle *page = (BM_PageHandle *) el->data;
		if (stats->dirtyFlags[el->index]) {
			forcePage(bm, page);
			el = el->next;
		}
	}
	return RC_OK;
}

// Buffer Manager Interface Access Pages
RC markDirty (BM_BufferPool *const bm, BM_PageHandle *const page){
    BP_Metadata *meta = bm->mgmtData;
    BM_LinkedListElement *el = NULL;
    if (!resolveByHandle(bm, page, &el)) {
        return RC_PAGE_NOT_IN_BUFFER;
    }

    meta->stats->dirtyFlags[el->index] = TRUE;
    printf("DEBUG: markDirty: pg@0x%08" PRIxPTR
        " { pageNum = %d }\n",
           (uintptr_t) page, page->pageNum);
    fflush(stdout);
    return RC_OK;
}

RC unpinPage (BM_BufferPool *const bm, BM_PageHandle *const page) {
	BP_Metadata *meta = bm->mgmtData;
    BM_LinkedListElement *el = NULL;
    if (!resolveByHandle(bm, page, &el)) {
        return RC_PAGE_NOT_IN_BUFFER;
    }

    meta->refCounter -= 1; //decrement buf mgr ref counter for thread use
    meta->stats->fixCounts[el->index] -= 1;
    return RC_OK;
}

RC forcePage (BM_BufferPool *const bm, BM_PageHandle *const page) {
    BP_Metadata *meta = bm->mgmtData;
	SM_FileHandle *storage = meta->storageManager;
    BM_LinkedListElement *el = NULL;
	if (!resolveByHandle(bm, page, &el)) {
        return RC_PAGE_NOT_IN_BUFFER;
    }

	printf("DEBUG: forcePage: pg@0x%08" PRIxPTR
	" { pageNum = %d, buf = 0x%08" PRIxPTR " }\n",
           (uintptr_t) page, page->pageNum, (uintptr_t) page->buffer);

	// ensure that we have enough pages before writing
	// recall that `pageNum` is zero-indexed
	ensureCapacity(page->pageNum + 1, storage);
	writeBlock(page->pageNum, storage, page->buffer);
	meta->stats->dirtyFlags[el->index] = false;

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
    BM_PageHandle *handle;
    if (resolveByPageNum(bm, pageNum, &el)) {
        handle = (BM_PageHandle *) el->data;
        stats->fixCounts[el->index] += 1;

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
        meta->inUse += 1;
        handle = (BM_PageHandle *) el->data;
        handle->pageNum = pageNum;
        stats->frameContents[el->index] = pageNum;
        stats->fixCounts[el->index] = 0;
        stats->dirtyFlags[el->index] = false;

        // update page number to element mapping
        HashMap_put(meta->pageMapping, pageNum, el);
        meta->strategyHandler->insert(bm, el);
        readBlock(pageNum, storage, handle->buffer);
    }

	// fetch page from memory
    meta->refCounter += 1; //increment buf mgr ref counter for thread use
    meta->strategyHandler->use(bm, el);

    // copy the result to caller variable
    if (page != NULL) {
        memcpy(page, handle, sizeof(BM_PageHandle));
    }

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
RC evict(BM_BufferPool *bm){
	BP_Metadata *meta = bm->mgmtData;
    BP_Statistics *stats = meta->stats;
    BM_LinkedList *pageTable = meta->pageTable;
    BM_LinkedListElement *el = meta->strategyHandler->elect(bm);
    if (el == NULL) {
        return RC_OK;
    }
    BM_PageHandle *page = (BM_PageHandle *) el->data;
    printf("DEBUG: evict: el@0x%08" PRIxPTR
        " { pageNum = %d, index = %d, data = 0x%08" PRIxPTR " }\n",
        (uintptr_t) el, page->pageNum, el->index, (uintptr_t) el->data);

    uint32_t pageNum = page->pageNum;
    if (meta->stats->dirtyFlags[el->index]) {
        forcePage(bm, page);
    }

    stats->frameContents[el->index] = 0;
    stats->dirtyFlags[el->index] = 0;
    stats->fixCounts[el->index] = 0;
    memset(page->buffer, 0, PAGE_SIZE);
    page->pageNum = -1;
    meta->inUse -= 1;

    LinkedList_remove(pageTable, el);
    HashMap_remove(meta->pageMapping, pageNum, NULL);

	return RC_OK;
}

static bool resolveByHandle(
        BM_BufferPool *bm,
        BM_PageHandle *handle,
        BM_LinkedListElement **el_out)
{
    if (handle == NULL) {
        return false;
    }

    return resolveByPageNum(bm, handle->pageNum, el_out);
}

static bool resolveByPageNum(
        BM_BufferPool *bm,
        PageNumber num,
        BM_LinkedListElement **el_out)
{
    if (el_out) {
        *el_out = NULL;
    }

    BP_Metadata *meta = bm->mgmtData;
    BM_LinkedListElement *el = NULL;
    if (!HashMap_get(meta->pageMapping, num, (void **) &el)) {
        return false;
    }

    if (!el) {
        fprintf(stderr, "resolveByPageNum: expected element to not be null\n");
        exit(1);
    }

    if (el_out) {
        *el_out = el;
    }
    return true;
}