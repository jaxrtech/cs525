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

RC initBufferPool(BM_BufferPool *const bm, const char *const pageFileName, 
		const int numPages, ReplacementStrategy strategy,
		void *stratData)
{
	printf("ASSIGNMENT 2 (Buffer Manager)\n\tCS 525 - SPRING 2020\n\tCHRISTOPHER MORCOM & JOSH BOWDEN\n\n");

    //Store BM_BufferPool Attributes
    bm->pageFile = pageFileName;
    bm->numPages = numPages;
    bm->strategy = strategy;
    bm->stratData = stratData;

    //set up bookkeeping data
    BP_Metadata *bmdata = malloc(sizeof(BP_Metadata));
    bmdata->clockCount = 0; //for clock replacement
    bmdata->refCounter = 0; //nothing using buffer yet
    bm->mgmtData = &bmdata; //link struct

    //set up pagetable
    BM_PageHandle *node = malloc(sizeof(BM_PageHandle)); 
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
	BM_PageHandle *node = bmdata->pageTable;
	int i;
    for (i = 0; i < bm->numPages; ++i){
    	BM_PageHandle *newnode = node->next;
    	free(node);
    	node = newnode;
    }
    free(bm->mgmtData); //free struct holding pg table
    free(bm); //free buffer manager
	return RC_OK;
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
	return -1;
}

RC unpinPage (BM_BufferPool *const bm, BM_PageHandle *const page){
	return -1;
}

RC forcePage (BM_BufferPool *const bm, BM_PageHandle *const page){
	return -1;
}

RC pinPage (BM_BufferPool *const bm, BM_PageHandle *const page, const PageNumber pageNum){
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
