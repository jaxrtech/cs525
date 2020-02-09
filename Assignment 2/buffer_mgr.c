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

SM_FileHandle fHandle;

// Buffer Manager Interface Pool Handling
RC initBufferPool(
        BM_BufferPool *const bm,
        const char *const pageFileName,
        const int numPages,             //num of buffer slots
        ReplacementStrategy strategy,
        void *stratData)
{
    printf("ASSIGNMENT 2 (Buffer Manager)\n\tCS 525 - SPRING 2020\n\tCHRISTOPHER MORCOM & JOSH BOWDEN\n\n");
    
    MAKE_POOL();

    //Store BM_BufferPool Attributes
    bm->pageFile = pageFileName;
    bm->numPages = numPages;
    bm->strategy = strategy;
    bm->stratData = stratData;

    //set up page directory
    bm->mgmtData = (struct BP_Metadata *) malloc(sizeof(BP_Metadata));
    //set up array of pages to hold numPages amount of pages
    (bm->mgmtData)->pageTable = (struct BM_PageHandle *)malloc(numPages*sizeof(BM_PageHandle));
    (bm->mgmtData)->LFU_Page = NULL; //nothing in use
    (bm->mgmtData)->LRU_Page = NULL; //nothing in use
    (bm->mgmtData)->refCounter = 0; //nothing in use
    (bm->mgmtData)->inUse = 0; //no pages in use
    //open pagefile
    openPageFile((char*)pageFileName, &fHandle); 
    
    return RC_OK;
}

RC shutdownBufferPool(BM_BufferPool *const bm){
    forceFlushPool(bm); //write all dirty pages to disk
    
    //use for freeing later
    BM_BufferPool *bp_tmp = bm;
    BP_Metadata *md_tmp = bm->mgmtData;
    BM_PageHandle *ph_tmp = md_tmp->pageTable;
    
    if (md_tmp->refCounter > 0){ return RC_FILE_IN_USE; } //check if any pages are pinned

    //Free buffer manager structs
    free(ph_tmp);
    free(md_tmp);
    free(bm);

    closePageFile(&fHandle);

    return RC_OK;
}

RC forceFlushPool(BM_BufferPool *const bm){
    int i;
    BM_PageHandle *page = (bm->mgmtData)->pageTable;
    for (i = 0; i < bm->numPages; i++){
        if (page[i].dirtyFlag == 1){ //check dirty bits for all pages in buffer
            forcePage(bm, &page[i]);  //write page to disk
            page[i].dirtyFlag = 0;   //unmark dirty bit
        }
    }
    return RC_OK;
}

/* check if page is loaded in buffer or read it from the pagefile
    - if the page is loaded, return a pointer to page frame 
        -- make sure to increment the pin/refCounter by one (makes it thread-safe)
    - else store it in a frame and return the pointer
        -- make sure to set the pin/refCounter to one (makes it thread-safe)
*/
RC pinPage (
        BM_BufferPool *const bm,
        BM_PageHandle *const page,
        const PageNumber pageNum)
{
    BM_PageHandle *pg = NULL;
    if ((pg = checkPool(bm, page))){ //check if page in buffer
        return pg;
    }
    else { //page is not in buffer. load page to frame (evict if buffer is full)
        BP_Metadata *ptable = bm->mgmtData;
        if (ptable->inUse == bm->numPages){ //buffer full. evict using specified strategy
            //evict
        }        
        //load the page into buffer

        //bm->mgmtData->pageTable[bm->mgmtData->inUse] = *page; //put page in next available frame 
        BM_PageHandle *newpg = &bm->mgmtData->pageTable[bm->mgmtData->inUse]; //get address of next available frame
        //set frame values
        newpg->pageNum = page->pageNum;
        newpg->data = page->data;
        newpg->dirtyFlag = page->dirtyFlag;
        newpg->refCounter = page->refCounter;

        return newpg;
    }
}

// Buffer Manager Interface Access Pages
RC markDirty (BM_BufferPool *const bm, BM_PageHandle *const page)
{
    BM_PageHandle *pg = NULL;
    if ((pg = checkPool(bm, page))){ //check buffer for page 
        pg->dirtyFlag=1;
        return RC_OK;
    }
    else { return RC_PAGE_NOT_IN_BUFFER; }

}

RC unpinPage (BM_BufferPool *const bm, BM_PageHandle *const page)
{
    return -1;
}

RC forcePage (BM_BufferPool *const bm, BM_PageHandle *const page)
{
    return -1;
}

// Statistics Interface
PageNumber *getFrameContents (BM_BufferPool *const bm)
{
    return NULL;
}

bool *getDirtyFlags (BM_BufferPool *const bm)
{
    return NULL;
}

int *getFixCounts (BM_BufferPool *const bm)
{
    return NULL;
}

int
getNumReadIO (BM_BufferPool *const bm)
{
    return -1;
}

int
getNumWriteIO (BM_BufferPool *const bm)
{
    return -1;
}

/************** HELPER FUNCTIONS ***************/

//Returns a pointer to page if it is in bm, NULL otherwise
BM_PageHandle *checkPool(BM_BufferPool const *bm, BM_PageHandle *page){
    int i;
    BM_PageHandle *pg = NULL;
    BP_Metadata *ptable = bm->mgmtData;
    for (i = 0; i < ptable->inUse; i++){
        pg = &((ptable->pageTable)[i]);
        if (pg->pageNum == page->pageNum){
            return pg;
        }
    }
    return pg;
}