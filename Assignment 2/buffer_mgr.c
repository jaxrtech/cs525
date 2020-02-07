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
    printf("ASSIGNMENT 2 (Buffer Manager)\n\tCS 525 - SPRING 2020\n\tCHRISTOPHER MORCOM & JOSH BOWDEN\n");

    //Store BM_BufferPool Attributes
    bm->pageFile = pageFileName;
    bm->numPages = numPages;
    bm->strategy = strategy;
    bm->stratData = stratData;

    //set up page directory
    bm->mgmtData = (struct BP_Metadata *) malloc(sizeof(BP_Metadata));
    //set up array of pages to hold numPages amount of pages
    (bm->mgmtData)->pageTable = (struct BM_PageHandle *)malloc(numPages*sizeof(BM_PageHandle));
    
    //open pagefile
    openPageFile((char*)pageFileName, &fHandle); 
    
    return RC_OK;
}

RC shutdownBufferPool(BM_BufferPool *const bm)
{
    forceFlushPool(bm); //write all dirty pages to disk
    
    //use for freeing later
    BP_Metadata *md_tmp = bm->mgmtData;
    BM_PageHandle *ph_tmp = md_tmp->pageTable;
    
    if (md_tmp->refCounter > 0){ return RC_FILE_IN_USE; } //check if any pages are pinned

    //Free buffer manager structs
    free(ph_tmp);
    free(md_tmp);

    return RC_OK;
}

RC forceFlushPool(BM_BufferPool *const bm)
{
    return -1;
}

// Buffer Manager Interface Access Pages
RC markDirty (BM_BufferPool *const bm, BM_PageHandle *const page)
{
    return -1;
}

RC unpinPage (BM_BufferPool *const bm, BM_PageHandle *const page)
{
    return -1;
}

RC forcePage (BM_BufferPool *const bm, BM_PageHandle *const page)
{
    return -1;
}

RC pinPage (
        BM_BufferPool *const bm,
        BM_PageHandle *const page,
        const PageNumber pageNum)
{
    return -1;
}

// Statistics Interface
PageNumber *
getFrameContents (BM_BufferPool *const bm)
{
    return NULL;
}

bool *
getDirtyFlags (BM_BufferPool *const bm)
{
    return NULL;
}

int *
getFixCounts (BM_BufferPool *const bm)
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
