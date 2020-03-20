#include <stdio.h>
#include <stdint.h>
#include <string.h>
#include <stdlib.h>

/* linux specific */
#include <stdbool.h>
#include <inttypes.h>

#include "dberror.h"
#include "storage_mgr.h"
#include "buffer_mgr.h"
#include "replacement_strategy.h"
#include "freespace.h"
#include "debug.h"

//Helper Functions
typedef enum BM_EvictMode {
    BM_EVICTMODE_FRESH,
    BM_EVICTMODE_REMOVE,
} BM_EvictMode;

static BM_LinkedListElement *evict(BM_BufferPool *bm, BM_EvictMode mode);

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
    meta->clock = 0; //for clock replacement
    meta->refCounter = 0; //nothing using buffer yet
    meta->inUse = 0;	  //no pages in use

    stats = malloc(sizeof(BP_Statistics));
    stats->diskReads = 0;
    stats->diskWrites = 0;
    stats->lastFrameContents = calloc(numPages, sizeof(PageNumber));
    stats->lastDirtyFlags = calloc(numPages, sizeof(bool));
    stats->lastFixCounts = calloc(numPages, sizeof(int));
    meta->stats = stats;

    // set up pagetable
    meta->pageDescriptors = LinkedList_create(numPages, sizeof(BP_PageDescriptor));

    // allocate memory pool
    meta->pageBuffer = calloc(numPages, PAGE_SIZE);
    for (uint32_t i = 0; i < numPages; i++) {
        BM_LinkedListElement *el = &meta->pageDescriptors->elementsMetaBuffer[i];
        BP_PageDescriptor *pd = (BP_PageDescriptor *) el->data;
        pd->handle.pageNum = -1;
        pd->handle.buffer = meta->pageBuffer + (i * PAGE_SIZE);
    }

    // allocate hash map
    meta->pageMapping = HashMap_create(128);

    // open the storage manager
    storageHandle = malloc(sizeof(SM_FileHandle));
    RC rc;
    if ((rc = createPageFile(pageFileName)) != RC_OK) {
        goto error;
    }
    if ((rc = openPageFile(pageFileName, storageHandle)) != RC_OK) {
        goto error;
    }
    meta->fileHandle = storageHandle;

    // initialize strategy handler
    meta->strategyHandler->init(bm);

	return RC_OK;

error:
    free(storageHandle);
    free(meta);
    return rc;
}

RC shutdownBufferPool(BM_BufferPool *const bm){
    forceFlushPool(bm); // write all dirty pages to disk
	BP_Metadata *meta = bm->mgmtData;
	if (meta->refCounter > 0) {
        // cannot shutdown because a page is still in use
		return RC_BM_IN_USE;
	}

	closePageFile(meta->fileHandle);

    BP_Statistics *stats = meta->stats;
    free(stats->lastFixCounts);
    stats->lastFixCounts = NULL;

    free(stats->lastDirtyFlags);
    stats->lastDirtyFlags = NULL;

    free(stats->lastFrameContents);
    stats->lastFixCounts = NULL;

    free(stats);

	LinkedList_free(meta->pageDescriptors);
	meta->pageDescriptors = NULL;

	HashMap_free(meta->pageMapping);
	meta->pageMapping = NULL;

	free(meta->pageBuffer);
	meta->pageBuffer = NULL;

	free(meta->fileHandle);
	meta->fileHandle = NULL;

	// handler struct itself is statically allocated, no need to free
	meta->strategyHandler->free(bm);

    free(meta);
    bm->mgmtData = NULL;

    // DO NOT FREE `bm` itself, since the test will free it

	return RC_OK;
}

RC forceFlushPool(BM_BufferPool *const bm){
	BP_Metadata *meta = bm->mgmtData;
    BM_LinkedList *pageTable = meta->pageDescriptors;
    BM_LinkedListElement *el = pageTable->head;
	while (el != pageTable->sentinel) {
        BP_PageDescriptor *pd = BM_DEREF_ELEMENT(el);
		if (pd->dirty) {
			forcePage(bm, &pd->handle);
		}
        el = el->next;
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

    BP_PageDescriptor *pd = BM_DEREF_ELEMENT(el);
    pd->dirty = TRUE;

#if LOG_DEBUG
    printf("DEBUG: markDirty: pg@0x%08" PRIxPTR
        " { pageNum = %d }\n",
           (uintptr_t) page, page->pageNum);
    fflush(stdout);
#endif

    return RC_OK;
}

RC unpinPage (BM_BufferPool *const bm, BM_PageHandle *const page) {
	BP_Metadata *meta = bm->mgmtData;
    BM_LinkedListElement *el = NULL;
    if (!resolveByHandle(bm, page, &el)) {
        return RC_PAGE_NOT_IN_BUFFER;
    }

    BP_PageDescriptor *pd = BM_DEREF_ELEMENT(el);
    meta->refCounter -= 1; //decrement buf mgr ref counter for thread use
    pd->fixCount -= 1;
    return RC_OK;
}

RC forcePage (BM_BufferPool *const bm, BM_PageHandle *const page) {
    BP_Metadata *meta = bm->mgmtData;
	SM_FileHandle *storage = meta->fileHandle;
    BM_LinkedListElement *el = NULL;
	if (!resolveByHandle(bm, page, &el)) {
        return RC_PAGE_NOT_IN_BUFFER;
    }
    BP_PageDescriptor *pd = BM_DEREF_ELEMENT(el);

#if LOG_DEBUG
    printf("DEBUG: forcePage: pg@0x%08" PRIxPTR
	" { pageNum = %d, buf = 0x%08" PRIxPTR " }\n",
           (uintptr_t) page, page->pageNum, (uintptr_t) page->buffer);
#endif

    // ensure that we have enough pages before writing
    // recall that `pageNum` is zero-indexed
    ensureCapacity(page->pageNum + 1, storage);
    writeBlock(page->pageNum, storage, page->buffer);
    pd->dirty = false;
    meta->stats->diskWrites += 1;

	return RC_OK;
}

RC pinPage (
        BM_BufferPool *const bm,
        BM_PageHandle *const page,
        const PageNumber pageNum)
{
	BP_Metadata *meta = bm->mgmtData;
    BM_LinkedList *pageTable = meta->pageDescriptors;
	SM_FileHandle *storage = meta->fileHandle;

	// check if page number exists
    BM_LinkedListElement *el;
    BP_PageDescriptor *pd;
    if (resolveByPageNum(bm, pageNum, &el)) {
        pd = BM_DEREF_ELEMENT(el);
        pd->fixCount += 1;

    } else {
        bool isFull = meta->inUse == bm->numPages;
        if (isFull) {
            // evict if buffer full
            // use `BM_EVICTMODE_FRESH` so that that links between the element
            // are not altered (we want to do an in-place update)
            el = evict(bm, BM_EVICTMODE_FRESH);
        } else {
            // find empty space in memory pool
            el = LinkedList_fresh(pageTable);
        }

	    if (el == NULL) {
	        fprintf(stderr, "pinPage: failed to pin page, evicted but list was full");
	        exit(1);
	    }
        meta->inUse += 1;
        pd = BM_DEREF_ELEMENT(el);
        pd->handle.pageNum = pageNum;
        pd->fixCount = 1;
        pd->dirty = false;

        // update page number to element mapping
        HashMap_put(meta->pageMapping, pageNum, el);
        if (!isFull) {
            // Only insert if the buffer was not full, and we're *not*
            // doing an insert in place
            meta->strategyHandler->insert(bm, el);
        }

        readBlock(pageNum, storage, pd->handle.buffer);
        meta->stats->diskReads += 1;
    }

	// fetch page from memory
    meta->refCounter += 1; //increment buf mgr ref counter for thread use
    meta->strategyHandler->use(bm, el);

    if (page) {
        *page = pd->handle;
    }

	return RC_OK;
}


// Statistics Interface
PageNumber *getFrameContents (BM_BufferPool *const bm) {
    BP_Metadata *meta = bm->mgmtData;
    BP_Statistics *stats = meta->stats;

    // Repopulate from linked list
    BM_LinkedList *descriptors = meta->pageDescriptors;
    BM_LinkedListElement *el = descriptors->sentinel->next;

    PageNumber *frameContents = stats->lastFrameContents;
    memset(frameContents, 0, sizeof(PageNumber) * bm->numPages);

    bool endFlag = false;
    for (uint32_t i = 0; i < bm->numPages; ++i) {
        if (!endFlag && el == descriptors->sentinel) {
            // Mark flag for when we pass the end of actually used pages
            endFlag = true;
        }

        if (!endFlag) {
            BP_PageDescriptor *pd = BM_DEREF_ELEMENT(el);
            frameContents[i] = pd->handle.pageNum;
            el = el->next;

        } else {
            // Fill the rest of the buffer with -1 for the page number
            // to represent an unused page
            frameContents[i] = -1;
        }
    }
    
    return frameContents;
}

bool *getDirtyFlags (BM_BufferPool *const bm){
    BP_Metadata *meta = bm->mgmtData;
    BP_Statistics *stats = meta->stats;

    // Repopulate from linked list
    BM_LinkedList *descriptors = meta->pageDescriptors;
    BM_LinkedListElement *el = descriptors->sentinel->next;

    bool *dirtyFlags = stats->lastDirtyFlags;
    memset(dirtyFlags, 0, sizeof(bool) * bm->numPages);

    bool endFlag = false;
    for (uint32_t i = 0; i < bm->numPages; ++i) {
        if (!endFlag && el == descriptors->sentinel) {
            // Mark flag for when we pass the end of actually used pages
            endFlag = true;
        }

        if (!endFlag) {
            BP_PageDescriptor *pd = BM_DEREF_ELEMENT(el);
            dirtyFlags[i] = pd->dirty;
            el = el->next;

        } else {
            // Fill the rest of the buffer with `false` by default
            dirtyFlags[i] = false;
        }
    }

    return dirtyFlags;
}

int *getFixCounts (BM_BufferPool *const bm){
    BP_Metadata *meta = bm->mgmtData;
    BP_Statistics *stats = meta->stats;

    // Repopulate from linked list
    BM_LinkedList *descriptors = meta->pageDescriptors;
    BM_LinkedListElement *el = descriptors->sentinel->next;

    int *fixCounts = stats->lastFixCounts;
    memset(fixCounts, 0, sizeof(int) * bm->numPages);

    bool endFlag = false;
    for (uint32_t i = 0; i < bm->numPages; ++i) {
        if (!endFlag && el == descriptors->sentinel) {
            // Mark flag for when we pass the end of actually used pages
            endFlag = true;
        }

        if (!endFlag) {
            BP_PageDescriptor *pd = BM_DEREF_ELEMENT(el);
            fixCounts[i] = pd->fixCount;
            el = el->next;

        } else {
            // Fill the rest of the buffer with 0 for unused pages
            fixCounts[i] = 0;
        }
    }

    return fixCounts;
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
static BM_LinkedListElement *evict(BM_BufferPool *bm, BM_EvictMode mode) {
	BP_Metadata *meta = bm->mgmtData;
    BM_LinkedListElement *el = meta->strategyHandler->elect(bm);
    if (el == NULL) {
        return NULL;
    }
    BP_PageDescriptor *pd = BM_DEREF_ELEMENT(el);
    uint32_t pageNum = pd->handle.pageNum;

#if LOG_DEBUG
    printf("DEBUG: evict: el@0x%08" PRIxPTR
        " { pageNum = %d, index = %d, data = 0x%08" PRIxPTR " }\n",
           (uintptr_t) el, pageNum, el->index, (uintptr_t) el->data);
#endif

    if (pd->dirty) {
        forcePage(bm, &pd->handle);
    }

    pd->dirty = false;
    pd->fixCount = 0;
    pd->handle.pageNum = -1;
    memset(pd->handle.buffer, 0, PAGE_SIZE);
    meta->inUse -= 1;

    HashMap_remove(meta->pageMapping, pageNum, NULL);
    if (mode == BM_EVICTMODE_FRESH) {
        return el;
    } else { // if (mode == BM_EVICTMODE_REMOVE) {
        LinkedList_remove(meta->pageDescriptors, el);
        return NULL;
    }
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