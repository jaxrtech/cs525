#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "rm_page.h"
#include "rm_macros.h"
#include "binfmt.h"
#include "btree_binfmt.h"
#include "btree.h"
#include "btree_mgr.h"
#include "record_mgr.h"

typedef struct IM_Metadata {
    RM_Metadata *recordManager;
} IM_Metadata;

static IM_Metadata *g_instance = NULL;

// init and shutdown index manager
RC initIndexManager(void *mgmtData IGNORE_UNUSED)
{
    printf("ASSIGNMENT 4 (Storage Manager)\n\tCS 525 - SPRING 2020\n\tCHRISTOPHER MORCOM & JOSH BOWDEN\n\n");

    RC rc;
    if (g_instance != NULL) {
        return RC_OK;
    }

    if (g_instance == NULL) {
        g_instance = malloc(sizeof(RM_Metadata));
    }

    if (g_instance == NULL) {
        PANIC("malloc: failed to allocate index manager metadata");
    }

    if ((rc = initRecordManager(NULL)) != RC_OK) {
        goto fail;
    }

    RM_Metadata *recordManager = RM_getInstance();
    if (recordManager == NULL) {
        rc = RC_IM_INIT_FAILED;
        goto fail;
    }

    goto success;

    fail:
        free(g_instance);
        g_instance = NULL;
        return rc;

    success:
        g_instance->recordManager = recordManager;
        return RC_OK;
}

RC shutdownIndexManager()
{
	if (g_instance != NULL) {
	    free(g_instance);
	    g_instance = NULL;
	}

	return RC_OK;
}

// create, destroy, open, and close an btree index
RC createBtree (char *idxId, DataType keyType, int n)
{
    if (idxId == NULL) {
        PANIC("'idxId' cannot be null. expect a unique index name.");
    }

    uint64_t indexNameLength = strlen(idxId);
    if (indexNameLength > BF_LSTRING_MAX_STRLEN) {
        return RC_RM_NAME_TOO_LONG;
    }

    RC rc;

    //check if index with same name already exists
    BTreeHandle *temp = NULL;

    /*
    if ((openBtree(&temp, idxId)) == RC_OK){
        closeBtree(temp);
        return RC_IM_KEY_ALREADY_EXISTS;
    }
    */

    //
    // Initialize root index node page
    //
    BM_BufferPool *pool = g_instance->recordManager->bufferPool;
    BP_Metadata *meta = pool->mgmtData;
    int dataPageNum = meta->fileHandle->totalNumPages;

    BM_PageHandle dataPageHandle = {};
    TRY_OR_RETURN(pinPage(pool, &dataPageHandle, dataPageNum));

    RM_Page *rootPage = RM_Page_init(dataPageHandle.buffer, dataPageNum, RM_PAGE_KIND_INDEX);
    rootPage->header.flags |= RM_PAGE_FLAGS_INDEX_ROOT;  // make page as root node
    rootPage->header.flags |= RM_PAGE_FLAGS_INDEX_LEAF;  // mark root as initially a leaf node

    TRY_OR_RETURN(forcePage(pool, &dataPageHandle));
    TRY_OR_RETURN(unpinPage(pool, &dataPageHandle));

    //
    // Setup information for the index descriptor into the disk format
    //
    struct IM_DESCRIPTOR_FORMAT_T indexDisk = IM_DESCRIPTOR_FORMAT;
    BF_SET_STR(indexDisk.idxName) = idxId;
    BF_SET_U8(indexDisk.idxKeyType) = keyType;
    BF_SET_U16(indexDisk.idxMaxEntriesPerNode) = n;
    BF_SET_U16(indexDisk.idxRootNodePageNum) = dataPageNum;

    uint16_t spaceRequired = BF_recomputePhysicalSize(
            (BF_MessageElement *) &indexDisk,
            BF_NUM_ELEMENTS(sizeof(indexDisk)));

    //
    // Open index descriptor page and reserve the required amount of space
    //
    BM_PageHandle pageHandle = {};
    TRY_OR_RETURN(pinPage(pool, &pageHandle, RM_PAGE_INDEX));

    RM_Page *systemPage = (RM_Page *) pageHandle.buffer;
    RM_PageTuple *tup = RM_Page_reserveTupleAtEnd(systemPage, spaceRequired);
    if (tup == NULL) {
        rc = RC_RM_NO_MORE_TUPLES;
        goto finally;
    }

    //
    // Write out of the index tuple data
    //
    void *tupleBuffer = &tup->dataBegin;
    BF_write((BF_MessageElement *) &indexDisk, tupleBuffer, BF_NUM_ELEMENTS(sizeof(indexDisk)));
    if ((rc = forcePage(pool, &pageHandle)) != RC_OK) {
        goto finally;
    }

    rc = RC_OK;

    finally:
        TRY_OR_RETURN(unpinPage(pool, &pageHandle));
        return rc;
}

RC openBtree (BTreeHandle **tree, char *idxId)
{
    PANIC_IF_NULL(tree);

    RC rc;
    BM_BufferPool *pool = g_instance->recordManager->bufferPool;

    struct IM_DESCRIPTOR_FORMAT_T indexMsg = {};
    BM_PageHandle pageHandle = {};
    if ((rc = IM_findIndex(pool, idxId, &pageHandle, NULL, &indexMsg)) != RC_OK) {
        return rc;
    }

    BTreeHandle *indexHandle = malloc(sizeof(BTreeHandle));
    indexHandle->idxId = idxId;
    indexHandle->keyType = BF_AS_U8(indexMsg.idxKeyType);

    IM_IndexMetadata *meta = malloc(sizeof(IM_IndexMetadata));
    IM_IndexMetadata_makeFromMessage(meta, &indexMsg);
    indexHandle->mgmtData = meta;

    *tree = indexHandle;
    return RC_OK;
}

RC closeBtree (BTreeHandle *tree)
{
    free(tree->mgmtData);
    free(tree);

    return RC_OK;
}

RC deleteBtree(char *idxId)
{
    PANIC_IF_NULL(idxId);
    BM_BufferPool *pool = g_instance->recordManager->bufferPool;

    return IM_deleteIndex(pool, idxId);
}

// access information about a b-tree
RC getNumNodes (BTreeHandle *tree, int *result)
{
    PANIC_IF_NULL(tree);
    PANIC_IF_NULL(result);

    BM_BufferPool *pool = g_instance->recordManager->bufferPool;
    IM_IndexMetadata *indexMeta = tree->mgmtData;

    return IM_getNumNodes(pool, indexMeta, result);
}

RC getNumEntries (BTreeHandle *tree, int *result)
{
	PANIC_IF_NULL(tree);
	PANIC_IF_NULL(result);

    BM_BufferPool *pool = g_instance->recordManager->bufferPool;
    IM_IndexMetadata *indexMeta = tree->mgmtData;
    RM_PageNumber rootPageNum = indexMeta->rootNodePageNum;

    // Find the left most leaf node
    RM_PageNumber startLeafNode = IM_getLeafNode(
            pool,
            rootPageNum,
            INT32_MIN,
            indexMeta->maxEntriesPerNode,
            NULL,
            NULL);

    // Count all the tuples
    int totalNumTuples = 0;

    RM_PageNumber pageNum = startLeafNode;
    BM_PageHandle pageHandle;
    bool done = false;
    do {
        TRY_OR_RETURN(pinPage(pool, &pageHandle, pageNum));
        RM_Page *page = (RM_Page *) pageHandle.buffer;

        totalNumTuples += page->header.numTuples;
        if (page->header.nextPageNum == RM_PAGE_NEXT_PAGENUM_UNSET) {
            done = true;
        } else {
            pageNum = page->header.nextPageNum;
        }

        TRY_OR_RETURN(unpinPage(pool, &pageHandle));

    } while (!done);

    *result = totalNumTuples;
    return RC_OK;
}

RC getKeyType (BTreeHandle *tree, DataType *result)
{
	PANIC_IF_NULL(tree);
	PANIC_IF_NULL(result);

	*result = tree->keyType;
	return RC_OK;
}

//look for leaf node that MIGHT contain a key
Node *getLeafNodePtr(BTreeHandle *tree, Value *key){
    //assume NULL key is to get the min value
    Node *node;
    //search tree here
    NOT_IMPLEMENTED();
    return node;
}

RC binarySearchNode(Node *node, Value *key, RID *result){
    //binary search the keys in a node and store the pointer to the value in result
    // will return RC_IM_KEY_NOT_FOUND if the node type is a leaf and key is nonexistent
    NOT_IMPLEMENTED();
}

// index access
RC findKey (BTreeHandle *tree, Value *key, RID *result)
{
    PANIC_IF_NULL(tree);
    PANIC_IF_NULL(key);
    PANIC_IF_NULL(result);

    if (key->dt != tree->keyType) {
        return RC_IM_KEY_DATA_TYPE_MISMATCH;
    }

    if (tree->keyType != DT_INT) {
        return RC_IM_KEY_DATA_TYPE_UNSUPPORTED;
    }

    BM_BufferPool *pool = g_instance->recordManager->bufferPool;
    IM_IndexMetadata *indexMeta = (IM_IndexMetadata *) tree->mgmtData;
    int32_t keyValue = key->v.intV;

    return IM_findEntry_i32(pool, indexMeta, keyValue, result, NULL, NULL);
}

RC insertKey (BTreeHandle *tree, Value *key, RID rid)
{
    PANIC_IF_NULL(tree);
    PANIC_IF_NULL(key);

    if (key->dt != tree->keyType) {
        return RC_IM_KEY_DATA_TYPE_MISMATCH;
    }

    if (tree->keyType != DT_INT) {
        return RC_IM_KEY_DATA_TYPE_UNSUPPORTED;
    }

    BM_BufferPool *pool = g_instance->recordManager->bufferPool;
    IM_IndexMetadata *indexMeta = (IM_IndexMetadata *) tree->mgmtData;

    return IM_insertKey_i32(pool, indexMeta, key->v.intV, rid);
}

RC deleteKey (BTreeHandle *tree, Value *key)
{
    PANIC_IF_NULL(tree);
    PANIC_IF_NULL(key);

    if (key->dt != tree->keyType) {
        return RC_IM_KEY_DATA_TYPE_MISMATCH;
    }

    if (tree->keyType != DT_INT) {
        return RC_IM_KEY_DATA_TYPE_UNSUPPORTED;
    }

    const int32_t keyValue = key->v.intV;
    BM_BufferPool *pool = g_instance->recordManager->bufferPool;
    IM_IndexMetadata *indexMeta = (IM_IndexMetadata *) tree->mgmtData;

    return IM_deleteKey_i32(pool, indexMeta, keyValue);
}

RC openTreeScan (BTreeHandle *tree, BT_ScanHandle **handle)
{
    PANIC_IF_NULL(tree);
    PANIC_IF_NULL(handle);

    BM_BufferPool *pool = g_instance->recordManager->bufferPool;
    IM_IndexMetadata *indexMeta = (IM_IndexMetadata *) tree->mgmtData;

    RM_PageNumber leafPageNum = IM_getLeafNode(
            pool,
            indexMeta->rootNodePageNum,
            INT32_MIN,
            indexMeta->maxEntriesPerNode,
            NULL,
            NULL);

    BT_ScanData *scandata = malloc(sizeof (BT_ScanData));
    scandata->currentNodePageNum = leafPageNum;
    scandata->currentSlotId = 0;
    scandata->currentPageHandle.pageNum = 0;
    scandata->currentPageHandle.buffer = NULL;
    scandata->finished = false;

    BT_ScanHandle *h = malloc(sizeof (BT_ScanHandle));
    h->tree = tree;
    h->mgmtData = scandata;
    *handle = h;

    return RC_OK;
}

RC nextEntry (BT_ScanHandle *handle, RID *result){
    PANIC_IF_NULL(handle);
    PANIC_IF_NULL(result);

    BM_BufferPool *pool = g_instance->recordManager->bufferPool;
    BT_ScanData *scandata = (BT_ScanData *) handle->mgmtData;

    if (scandata->currentPageHandle.buffer == NULL) {
        TRY_OR_RETURN(pinPage(
                pool,
                &scandata->currentPageHandle,
                scandata->currentNodePageNum));
    }

    RM_Page *currentPage = (RM_Page *) scandata->currentPageHandle.buffer;
    uint16_t numTuples = currentPage->header.numTuples;
    if (scandata->currentSlotId >= numTuples) {
        // Free current page. Setup new page num but wait for the next iteration
        // to open it
        TRY_OR_RETURN(unpinPage(pool, &scandata->currentPageHandle));

        int nextPageNumQ = currentPage->header.nextPageNum;
        if (nextPageNumQ == RM_PAGE_NEXT_PAGENUM_UNSET) {
            return RC_IM_NO_MORE_ENTRIES;
        }

        if (nextPageNumQ < 0) { PANIC("expected next page num to be otherwise assigned"); }

        uint16_t nextPageActual = nextPageNumQ;
        scandata->currentNodePageNum = nextPageActual;
        scandata->currentSlotId = 0;
        scandata->currentPageHandle.buffer = NULL;
        scandata->currentPageHandle.pageNum = 0;

        TRY_OR_RETURN(pinPage(
                pool,
                &scandata->currentPageHandle,
                scandata->currentNodePageNum));

        currentPage = (RM_Page *) scandata->currentPageHandle.buffer;
    }

    RM_PageTuple *tup = RM_Page_getTuple(currentPage, scandata->currentSlotId, NULL);
    IM_ENTRY_FORMAT_T entry;
    IM_readEntry_i32(tup, &entry);

    *result = IM_makeRidFromEntry(&entry);
    scandata->currentSlotId++;

    return RC_OK;
}

RC closeTreeScan (BT_ScanHandle *handle){
	BT_ScanData *data = handle->mgmtData;
    free(data);
    free(handle);
    return RC_OK;
}

// debug and test functions
char *printTree (BTreeHandle *tree){
	NOT_IMPLEMENTED();
}