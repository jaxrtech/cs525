#include <stdio.h>
#include <stdint.h>
#include <string.h>

/* linux specific */
#include <unistd.h>
#include <errno.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <stdbool.h>

#include "dberror.h"
#include "storage_mgr.h"
#include "buffer_mgr.h"

// Buffer Manager Interface Pool Handling
RC initBufferPool(
        BM_BufferPool *const bm,
        const char *const pageFileName,
        const int numPages,
        ReplacementStrategy strategy,
        void *stratData)
{
    return -1;
}

RC shutdownBufferPool(BM_BufferPool *const bm)
{
    return -1;
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
