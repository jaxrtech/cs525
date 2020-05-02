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

    g_instance = malloc(sizeof(RM_Metadata));
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
    indexHandle->mgmtData = meta;
    meta->rootNodePageNum = BF_AS_U16(indexMsg.idxRootNodePageNum);
    meta->maxEntriesPerNode = BF_AS_U16(indexMsg.idxMaxEntriesPerNode);

    *tree = indexHandle;
    return RC_OK;
}

RC closeBtree (BTreeHandle *tree)
{
    free(tree->mgmtData);
    free(tree);

    return RC_OK;
}

RC deleteBtree (char *idxId){
    //i assume page 3 is a page where each tuple points to a btree. we iterate through and delete the tree.
	NOT_IMPLEMENTED();

    BM_BufferPool *pool = g_instance->recordManager->bufferPool;
    BM_PageHandle handle;

    TRY_OR_RETURN(pinPage(pool, &handle, RM_PAGE_KIND_INDEX)); //pin page containing btrees
    RM_Page *page = (RM_Page *) handle.buffer;

    //TODO: delete tuple Here

    TRY_OR_RETURN(markDirty(pool, &handle));
    TRY_OR_RETURN(unpinPage(pool, &handle));
    return RC_OK;
}

// access information about a b-tree
typedef struct IM_GetNumNodes_Elem {
    RM_PageNumber pageNum;
    RM_PageSlotId nextSlotId;
} IM_GetNumNodes_Elem;

RC getNumNodes (BTreeHandle *tree, int *result)
{
    PANIC_IF_NULL(tree);
    PANIC_IF_NULL(result);

    BM_BufferPool *pool = g_instance->recordManager->bufferPool;
    IM_IndexMetadata *indexMeta = tree->mgmtData;
    RM_PageNumber rootPageNum = indexMeta->rootNodePageNum;
    
    // Allocate a stack to push parent nodes when we need to traverse it's
    // children
    uint32_t stackCapacity = 16;
    uint32_t stackLen = 0;
    IM_GetNumNodes_Elem *stack = calloc(stackCapacity, sizeof(*stack));

    int numNodes = 1;

    RM_PageNumber curPageNum = rootPageNum;
    BM_PageHandle curPageHandle;
    RM_PageSlotId nextSlotId = 0;
    do {
        TRY_OR_RETURN(pinPage(pool, &curPageHandle, curPageNum));
        RM_Page *curPage = (RM_Page *) curPageHandle.buffer;

        bool isCurPageRoot = IS_FLAG_SET(curPage->header.flags, RM_PAGE_FLAGS_INDEX_ROOT);
        bool isCurPageLeaf = IS_FLAG_SET(curPage->header.flags, RM_PAGE_FLAGS_INDEX_LEAF);

        if (isCurPageRoot && isCurPageLeaf) {
            // There is only one node
            break;
        }

        if (isCurPageLeaf) {
            PANIC("expected inner node but got leaf node");
        }

        // Current node is an inner node
        //
        // Determine if this inner node has other inner node children
        // or if it only has leaf nodes as children, since if it's only
        // leaf nodes, we can just use the `numTuples`

        RM_PageTuple *tup = RM_Page_getTuple(curPage, nextSlotId, NULL);
        IM_ENTRY_FORMAT_T entry;
        IM_readEntry_i32(tup, &entry, sizeof(entry));

        RM_PageNumber slotPageNum = BF_AS_U16(entry.idxEntryRidPageNum);
        BM_PageHandle slotPageHandle;
        TRY_OR_RETURN(pinPage(pool, &slotPageHandle, slotPageNum));
        RM_Page *slotPage = (RM_Page *) slotPageHandle.buffer;

        if (slotPage->header.kind != RM_PAGE_KIND_INDEX) {
            PANIC("expected page referenced by slot of non-leaf node "
                  "to be an index page");
        }

        if (IS_FLAG_SET(slotPage->header.flags, RM_PAGE_FLAGS_INDEX_LEAF)) {
            // The rest of the slots should be leaf nodes, therefore we can
            // just count the slots on this inner node
            // Add +1 for the nextPage pointer
            numNodes += curPage->header.numTuples + 1;

            // Attempt to pop the parent to read the rest of the parent inner slots
            bool finished = false;
            if (stackLen > 0) {
                IM_GetNumNodes_Elem *el = &stack[stackLen - 1];
                curPageNum = el->pageNum;
                nextSlotId = el->nextSlotId;
            } else {
                finished = true;
            }

            TRY_OR_RETURN(unpinPage(pool, &slotPageHandle));
            if (finished) {
                break;
            }

            continue;
        }

        // If the first slot is not a leaf, then this inner node must have inner
        // node children. We need to traverse through each slot then.
        //
        // Push the current node to be the parent node and increment the next
        // slot id.
        if (stackLen == stackCapacity) {
            stackCapacity = stackCapacity << 1u;
            stack = realloc(stack, stackCapacity);
        }
        stack[stackLen].pageNum = curPageNum;
        stack[stackLen].nextSlotId = nextSlotId + 1;
        stackLen++;

        // Traverse the first inner node
        curPageNum = slotPageNum;
        nextSlotId = 0;
        TRY_OR_RETURN(unpinPage(pool, &slotPageHandle));

    } while (true);

    *result = numNodes;
    return RC_OK;
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

    if (key->dt != tree->keyType) {
        return RC_IM_KEY_DATA_TYPE_MISMATCH;
    }

    if (tree->keyType != DT_INT) {
        return RC_IM_KEY_DATA_TYPE_UNSUPPORTED;
    }

    RC rc;
    BM_BufferPool *pool = g_instance->recordManager->bufferPool;
    IM_IndexMetadata *indexMeta = (IM_IndexMetadata *) tree->mgmtData;
    const uint16_t maxEntriesPerNode = indexMeta->maxEntriesPerNode;

    int32_t keyValue = key->v.intV;
    RM_PageNumber leafPageNum = IM_getLeafNode(
            pool,
            indexMeta->rootNodePageNum,
            keyValue,
            maxEntriesPerNode,
            NULL,
            NULL);

    BM_PageHandle leafPageHandle;
    TRY_OR_RETURN(pinPage(pool, &leafPageHandle, leafPageNum));
    RM_Page *leafPage = (RM_Page *) leafPageHandle.buffer;

    IM_ENTRY_FORMAT_T entry;
    bool found = IM_getEntryIndex(
            keyValue,
            leafPage,
            maxEntriesPerNode,
            &entry,
            NULL);

    if (!found) {
        rc = RC_IM_KEY_NOT_FOUND;
        goto finally;
    }

    result->page = BF_AS_U16(entry.idxEntryRidPageNum);
    result->slot = BF_AS_U16(entry.idxEntryRidSlot);
    rc = RC_OK;

finally:
    TRY_OR_RETURN(unpinPage(pool, &leafPageHandle));
    return rc;
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

RC deleteKey (BTreeHandle *tree, Value *key){
    //delete key and record pointer from tree and rebalance or RC_IM_KEY_NOT_FOUND
    NOT_IMPLEMENTED();
}
RC openTreeScan (BTreeHandle *tree, BT_ScanHandle **handle){
    BT_ScanHandle *h = malloc(sizeof (BT_ScanHandle));
	h->tree = tree;

    BT_ScanData *scandata = malloc(sizeof (BT_ScanData));
    scandata->currentNode = getLeafNodePtr(tree, NULL); //get leftmost leaf node
    scandata->nodeIdx = 0;
    (*handle)->mgmtData = scandata;

    return RC_OK;
}

RC nextEntry (BT_ScanHandle *handle, RID *result){
    //loads current node and stores the RID pointer in result
    BT_ScanData *sd = handle->mgmtData;
    int idx = sd->nodeIdx;
    /* * load current node and current index
	   * store resultant RID in result
       * check if idx < fill
            ** if not, RC_OK
            ** else, check last ptr in node
                *** if NULL, RC_IM_NO_MORE_ENTRIES
                *** else, sd->nodeIdx = 0; curren node = last ptr of prev node.
    */
    NOT_IMPLEMENTED();

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