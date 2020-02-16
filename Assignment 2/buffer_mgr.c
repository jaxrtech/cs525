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

static void freeBufferPageTable(const BM_BufferPool *bm);

RC initBufferPool(
        BM_BufferPool *const bm,
        const char *const pageFileName,
		const int numPages,
		ReplacementStrategy strategy,
		void *stratData)
{
	printf("ASSIGNMENT 2 (Buffer Manager)\n\tCS 525 - SPRING 2020\n\tCHRISTOPHER MORCOM & JOSH BOWDEN\n\n");

	BP_Metadata *bmdata = NULL;
    SM_FileHandle *storageHandle = NULL;
	BM_PageHandle *node = NULL;

    //Store BM_BufferPool Attributes
    bm->pageFile = pageFileName;
    bm->numPages = numPages;
    bm->strategy = strategy;
    bm->stratData = stratData;

    //set up bookkeeping data
    bmdata = malloc(sizeof(BP_Metadata));
    bmdata->clockCount = 0; //for clock replacement
    bmdata->refCounter = 0; //nothing using buffer yet
    bmdata->inUse = 0;		//no pages in use
    bm->mgmtData = &bmdata; //link struct

    //open the storage manager
    storageHandle = malloc(sizeof(SM_FileHandle));
    RC result;
    if ((result = openPageFile(pageFileName, storageHandle)) != RC_OK) {
        free(storageHandle);
        free(bmdata);
        return result;
    }
    bmdata->storageManager = storageHandle;

    //set up pagetable
    node = malloc(sizeof(BM_PageHandle));
	node->refCounter = 0;
	node->dirtyFlag = 0;
	node->pageNum = -1;
	node->data = NULL;
	node->next = NULL;
	node->prev = NULL;

    int i;
    bmdata->pageTable = node;

    for (i = 0; i < numPages-1; ++i){
    	BM_PageHandle *newnode = malloc(sizeof(BM_PageHandle));
    	node->refCounter = 0;
		node->dirtyFlag = 0;
		node->pageNum = 0;
		node->data = NULL;
    	node->next = newnode;
    	newnode->prev = node;
    	node = node->next;
    }
    node->next = bmdata->pageTable; //close doubly linked list
    node->next->prev = node;

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
	//forcepage() on all pages in buffer
	BP_Metadata *bmdata = bm->mgmtData;
	BM_PageHandle *node = bmdata->pageTable;
	int i;
	for (i = 0; i < bm->numPages; ++i){
		if (node->dirtyFlag == 1){
			forcePage(bm, node);
			node = node->next;
		}
	}
	return RC_OK;
}

// Buffer Manager Interface Access Pages
RC markDirty (BM_BufferPool *const bm, BM_PageHandle *const page){
	BM_PageHandle *pg = checkPool(bm, page);
	if(pg){
		pg->dirtyFlag = 1;
		return RC_OK;
	}
	return RC_PAGE_NOT_IN_BUFFER;
}

RC unpinPage (BM_BufferPool *const bm, BM_PageHandle *const page){
	BM_PageHandle *pg = checkPool(bm, page);
	BP_Metadata *bmdata = bm->mgmtData;
	if(pg){
		pg->refCounter -= 1;
		updatePageTable(bm, bmdata->pageTable);
		bmdata->refCounter-=1; //decrement buf mgr ref counter for thread use
		return RC_OK;
	}
	return RC_PAGE_NOT_IN_BUFFER;
}

RC forcePage (BM_BufferPool *const bm, BM_PageHandle *const page){
    BP_Metadata *bmdata = bm->mgmtData;
	SM_FileHandle *storage = bmdata->storageManager;
	writeBlock(page->pageNum, storage, page->data);
	updatePageTable(bm, bmdata->pageTable);
	return -1;
}

RC pinPage (BM_BufferPool *const bm, BM_PageHandle *const page, const PageNumber pageNum){
	BP_Metadata *bmdata = bm->mgmtData;
	BM_PageHandle *pg = checkPool(bm, page);
	if(pg){ //if page in buffer, increment pin count
		pg->refCounter += 1;
	}
	else if (bmdata->inUse == bm->numPages){ //evict if buffer full
		evict(bm);
	}
	//fetch page from memory
	bmdata->refCounter+=1; //increment buf mgr ref counter for thread use
	updatePageTable(bm, bmdata->pageTable);
	return -1;
}


// Statistics Interface
PageNumber *getFrameContents (BM_BufferPool *const bm){
	return NULL;
}

bool *getDirtyFlags (BM_BufferPool *const bm){
	return false;
}

int *getFixCounts (BM_BufferPool *const bm){
	return NULL;
}

int getNumReadIO (BM_BufferPool *const bm){
	return -1;
}

int getNumWriteIO (BM_BufferPool *const bm){
	return -1;
}


/*		HELPER FUNCTIONS		*/
BM_PageHandle *checkPool(BM_BufferPool *const bm, BM_PageHandle *page){
	BP_Metadata *bmdata = bm->mgmtData;
	return NULL;
}

RC evict(BM_BufferPool *const bm){
	//check page in buffer handled by calling function
	//pages will be sorted based on update function so eviction is standard unless it is clock 
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
