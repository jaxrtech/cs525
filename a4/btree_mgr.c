#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>

#include "btree_mgr.h"
#include "record_mgr.h"
#include "storage_mgr.h"
//#include "tables.h"

#include "buffer_mgr.h"
#include "rm_page.h"
#include "rm_macros.h"
#include "binfmt.h"
#include "btree_binfmt.h"

typedef struct IM_Metadata {
    RM_Metadata *recordManager;
} IM_Metadata;

typedef struct IM_IndexMetadata {
    RM_PageNumber rootNodePageNum;
    uint16_t maxEntriesPerNode;
} IM_IndexMetadata;

static IM_Metadata *g_instance = NULL;

static uint16_t IM_getEntryInsertionIndex(
        int32_t keyValue,
        RM_Page *page,
        uint16_t maxEntriesPerNode);

static RM_PageNumber IM_getLeafNode(
        RM_PageNumber rootPageNum,
        int32_t searchKey,
        uint16_t maxEntriesPerNode,
        RM_PageNumber *parent_out);

RM_PageTuple *
RM_reserveTupleAtIndex(RM_Page *page, const uint16_t len, uint16_t slotNum);

// called by record manager -- do not mark as `static`
RC IM_writeIndexPage(BM_BufferPool *pool)
{
    BM_PageHandle pageHandle = {};
    TRY_OR_RETURN(pinPage(pool, &pageHandle, RM_PAGE_INDEX));

    RM_Page_init(pageHandle.buffer, RM_PAGE_INDEX, RM_PAGE_KIND_INDEX);

    TRY_OR_RETURN(forcePage(pool, &pageHandle));
    TRY_OR_RETURN(unpinPage(pool, &pageHandle));

    return RC_OK;
}

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

    uint16_t spaceRequired = BF_recomputeSize(
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

void IM_readEntryI32(RM_PageTuple *tup, IM_ENTRY_FORMAT_T *result, size_t n)
{
    if (tup == NULL) { PANIC("'tup' cannot be null"); }
    if (result == NULL) { PANIC("'result' cannot be null"); }
    if (n == 0) { PANIC("'size' must be greater than zero byets"); }

    *result = IM_ENTRY_FORMAT_OF_INT32;
    uint16_t read = BF_read(
            (BF_MessageElement *) result,
            &tup->dataBegin,
            BF_NUM_ELEMENTS(n));
    if (read > tup->len) {
        PANIC("buffer overrun: got %d bytes, but buffer is %d bytes",
                read,
                tup->len);
    }
}

RC IM_findIndex(
        char *name,
        BM_PageHandle *page_out,
        struct RM_PageTuple **tup_out,
        struct IM_DESCRIPTOR_FORMAT_T *msg_out)
{
    size_t nameLength = strlen(name);
    BM_BufferPool *pool = g_instance->recordManager->bufferPool;

    //store the schema page in pageHandle
    BM_PageHandle pageHandle = {};
    TRY_OR_RETURN(pinPage(pool, &pageHandle, RM_PAGE_INDEX));

    //open the schema page and store the header and data
    RM_Page *pg = (RM_Page*) pageHandle.buffer;
    RM_PageHeader *hdr = &pg->header;
    //as long as this isn't zero, it is an offset from the header to the tuple
    //increment it by 2 bytes to get the next pointer
    RM_PageSlotPtr *off; //deref this address for the offset

    //find the pointer in the data that points to the proper table tuple (or error if table doesn't exist)
    RM_PageTuple *tup = NULL;
    uint16_t num = hdr->numTuples;
    bool hit = false;
    struct IM_DESCRIPTOR_FORMAT_T indexMsg = {};
    for (int i = 0; i < num; i++) {
        size_t slot = i * sizeof(RM_PageSlotPtr);
        off = (RM_PageSlotPtr *) (&pg->dataBegin + slot);
        if (*off >= RM_PAGE_DATA_SIZE) {
            PANIC("tuple offset cannot be greater than page data size");
        }

        //printf("openTable: [tup#%d] pg = %p, pg->data = %p, slot_off = %d, off = %d\n", i, pg, &pg->dataBegin, slot, *off);
        //fflush(stdout);
        tup = (RM_PageTuple *) (&pg->dataBegin + *off);

        indexMsg = IM_DESCRIPTOR_FORMAT;
        BF_read((BF_MessageElement *) &indexMsg, &tup->dataBegin, BF_NUM_ELEMENTS(sizeof(IM_DESCRIPTOR_FORMAT)));
        int idxNameLength = BF_STRLEN(indexMsg.idxName) - 1; // BF will store the extra '\0'
        if (idxNameLength <= 0) {
            PANIC("bad index name length = %d", idxNameLength);
        }

        if (nameLength == idxNameLength
            && memcmp(BF_AS_STR(indexMsg.idxName), name, idxNameLength) == 0)
        {
            hit = true;
            break;
        }
    }

    if (!hit) {
        return RC_IM_KEY_NOT_FOUND;
    }

    if (page_out != NULL) {
        *page_out = pageHandle;
    }

    if (tup_out != NULL) {
        *tup_out = tup;
    }

    if (msg_out != NULL) {
        memcpy(msg_out, &indexMsg, sizeof(IM_DESCRIPTOR_FORMAT));
    }

    return RC_OK;
}

RC openBtree (BTreeHandle **tree, char *idxId)
{
    if (tree == NULL) { PANIC("'tree' cannot be null"); }

    RC rc;

    struct IM_DESCRIPTOR_FORMAT_T indexMsg = {};
    BM_PageHandle pageHandle = {};
    if ((rc = IM_findIndex(idxId, &pageHandle, NULL, &indexMsg)) != RC_OK) {
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
    free(tree);
    // TODO: Free any mgmt data

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
RC getNumNodes (BTreeHandle *tree, int *result){
	NOT_IMPLEMENTED();
}
RC getNumEntries (BTreeHandle *tree, int *result){
	NOT_IMPLEMENTED();
}
RC getKeyType (BTreeHandle *tree, DataType *result){
	NOT_IMPLEMENTED();
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
RC findKey (BTreeHandle *tree, Value *key, RID *result){
    //get datatype from header info
    //navigate tree and return RID with search key (Value *key) or RC_IM_KEY_NOT_FOUND
    Node *leaf = getLeafNodePtr(tree, key);
    RC rc = binarySearchNode(leaf, key, result);
    return rc;
}

typedef struct IM_SplitLeafNodeCtx {
    RM_PageNumber leftPageNum;
    RM_PageNumber rightPageNum;
    bool isTargetLeft;
    RM_PageSlotId targetSlot;
} IM_SplitLeafNodeCtx;

RC IM_insertAndSplitLeafNode(
        RM_Page *oldPage,
        int32_t keyValue,
        IM_ENTRY_FORMAT_T *entry,
        uint16_t entrySize,
        BM_BufferPool *pool,
        uint16_t maxEntriesPerNode,
        IM_SplitLeafNodeCtx *ctx_out)
{
    BP_Metadata *meta = pool->mgmtData;
    RM_PageHeader *pageHeader = &oldPage->header;
    const uint16_t initialNumEntries = pageHeader->numTuples;

    // Determine the index at which the entry would normally be inserted
    uint16_t targetSlotIdx = IM_getEntryInsertionIndex(keyValue, oldPage, maxEntriesPerNode);

    // Allocate the left anf right leaf nodes
    int leftPageNum = meta->fileHandle->totalNumPages;
    int rightPageNum = leftPageNum + 1;

    BM_PageHandle leftPageHandle = {};
    BM_PageHandle rightPageHandle = {};
    TRY_OR_RETURN(pinPage(pool, &leftPageHandle, leftPageNum));
    TRY_OR_RETURN(pinPage(pool, &rightPageHandle, rightPageNum));

    // Initialize pages
    RM_Page *leftPage = RM_Page_init(leftPageHandle.buffer, leftPageNum, RM_PAGE_KIND_INDEX);
    RM_Page *rightPage = RM_Page_init(rightPageHandle.buffer, leftPageNum, RM_PAGE_KIND_INDEX);

    // Unset leaf flag from the old now inner node
    UNSET_FLAG(oldPage->header.flags, RM_PAGE_FLAGS_INDEX_LEAF);
    SET_FLAG(oldPage->header.flags, RM_PAGE_FLAGS_INDEX_INNER);

    // Set indexing flags
    SET_FLAG(leftPage->header.flags, RM_PAGE_FLAGS_INDEX_LEAF);
    SET_FLAG(rightPage->header.flags, RM_PAGE_FLAGS_INDEX_LEAF);

    // Link the left node to the right node, and the right node to the original next
    leftPage->header.nextPageNum = rightPageNum;
    rightPage->header.nextPageNum = oldPage->header.nextPageNum;

    // Check if the max number of entries is even.
    // If so, the left node gets the extra entry.
    uint16_t numLeftFill;
    bool isMaxEven = maxEntriesPerNode % 2 == 0;
    if (isMaxEven) {
        // ex: if the max == 2, then 2 nodes left, 1 node right
        numLeftFill = maxEntriesPerNode;
    } else {
        // ex: if the max == 5, then 3 nodes left, 3 nodes right
        numLeftFill = (maxEntriesPerNode + 1) / 2;
    }

    // Fill the left until it's "full"
    uint16_t oldPageShift = 0; // use when we pass the insertion point, to offset items
    uint16_t i;
    for (i = 0; i < numLeftFill; i++) {
        if (i != targetSlotIdx) {
            // Get the old node
            RM_PageTuple *oldTup = RM_Page_getTuple(oldPage, i + oldPageShift, NULL);
            RM_PageSlotLength len = oldTup->len;

            // Reserve the tuple in the new node to copy the old one
            RM_PageTuple *newTup = RM_Page_reserveTupleAtEnd(leftPage, len);

            // Copy the entry over
            memcpy(&newTup->dataBegin, &oldTup->dataBegin, len);
        }
        else {
            // This is where the node we're inserting will go
            // Increment the page shift by one
            oldPageShift++;

            RM_PageSlotLength len = entrySize;
            RM_PageTuple *newTup = RM_Page_reserveTupleAtEnd(leftPage, len);

            // Write out new entry
            uint16_t wrote = BF_write(
                    (BF_MessageElement *) &entry,
                    &newTup->dataBegin,
                    BF_NUM_ELEMENTS(entrySize));
            if (wrote > len) { PANIC("buffer overflow"); }
        }
    }

    // Fill the rest of the tuples into the right node
    for (i = numLeftFill; i < initialNumEntries; i++) {
        if (i != targetSlotIdx) {
            // Get the old node
            RM_PageTuple *oldTup = RM_Page_getTuple(oldPage, i + oldPageShift, NULL);
            RM_PageSlotLength len = oldTup->len;

            // Reserve the tuple in the new node to copy the old one
            RM_PageTuple *newTup = RM_Page_reserveTupleAtEnd(rightPage, len);

            // Copy the entry over
            memcpy(&newTup->dataBegin, &oldTup->dataBegin, len);
        }
        else {
            // This is where the node we're inserting will go
            // Increment the page shift by one
            oldPageShift++;

            RM_PageSlotLength len = entrySize;
            RM_PageTuple *newTup = RM_Page_reserveTupleAtEnd(rightPage, len);

            // Write out new entry
            uint16_t wrote = BF_write(
                    (BF_MessageElement *) &entry,
                    &newTup->dataBegin,
                    BF_NUM_ELEMENTS(entrySize));
            if (wrote > len) { PANIC("buffer overflow"); }
        }
    }

    TRY_OR_RETURN(forcePage(pool, &leftPageHandle));
    TRY_OR_RETURN(forcePage(pool, &rightPageHandle));

    TRY_OR_RETURN(unpinPage(pool, &leftPageHandle));
    TRY_OR_RETURN(unpinPage(pool, &rightPageHandle));

    if (ctx_out != NULL) {
        ctx_out->leftPageNum = leftPageNum;
        ctx_out->rightPageNum = rightPageNum;
        ctx_out->isTargetLeft = targetSlotIdx < numLeftFill;
        ctx_out->targetSlot = targetSlotIdx % numLeftFill;
    }

    return RC_OK;
}

RC insertKey (BTreeHandle *tree, Value *key, RID rid){
    if (tree == NULL) { PANIC("'tree' cannot be null"); }
    if (key == NULL) { PANIC("'key' cannot be null"); }

    if (key->dt != tree->keyType) {
        return RC_IM_KEY_DATA_TYPE_MISMATCH;
    }

    if (tree->keyType != DT_INT) {
        return RC_IM_KEY_DATA_TYPE_UNSUPPORTED;
    }

    const int keyValue = key->v.intV;
    BM_BufferPool *pool = g_instance->recordManager->bufferPool;
    IM_IndexMetadata *indexMeta = (IM_IndexMetadata *) tree->mgmtData;

    //
    // Create the entry node for usage later
    //

    IM_ENTRY_FORMAT_T entry = IM_ENTRY_FORMAT_OF_INT32;
    BF_SET_I32(entry.idxEntryKey) = keyValue;
    BF_SET_U16(entry.idxEntryRidPage) = rid.page;
    BF_SET_U16(entry.idxEntryRidSlot) = rid.slot;

    const uint16_t entrySize = BF_recomputeSize(
            (BF_MessageElement *) &entry,
            BF_NUM_ELEMENTS(sizeof(entry)));

    //
    // Find the leaf node to insert into starting at the root node
    //

    const RM_PageNumber rootPageNum = indexMeta->rootNodePageNum;
    const uint16_t maxEntriesPerNode = indexMeta->maxEntriesPerNode;

    // Find the leaf node where we need to insert the entry
    RM_PageNumber parentPageNum;
    RM_PageNumber leafNodePageNum = IM_getLeafNode(
            rootPageNum,
            keyValue,
            maxEntriesPerNode,
            &parentPageNum);

    // Calculate initial number of entries in the leaf node
    BM_PageHandle leafNodePageHandle = {};
    TRY_OR_RETURN(pinPage(pool, &leafNodePageHandle, leafNodePageNum));
    RM_Page *leafNodePage = (RM_Page *) leafNodePageHandle.buffer;
    uint16_t initialNumEntries = leafNodePage->header.numTuples;

    // HACK: We're going to assume that `maxEntriesPerNode` is <= max number of tuples
    //       that can fit in a page
    bool hasSpace = initialNumEntries < maxEntriesPerNode;
    if (hasSpace) {
        RM_PageTuple *targetTup = NULL;
        if (initialNumEntries == 0) {
            // Just reserve a new tuple since the node is empty (must be a new root node)
            targetTup = RM_Page_reserveTupleAtEnd(leafNodePage, entrySize);
        }
        else { // if (initialNumEntries > 0)
            // Determine where we need to insert the entry (using linear search)
            uint16_t slotNum = IM_getEntryInsertionIndex(keyValue, leafNodePage, maxEntriesPerNode);
            targetTup = RM_reserveTupleAtIndex(
                    leafNodePage,
                    entrySize,
                    slotNum);
        }

        BF_write((BF_MessageElement *) &entry,
                 (void *) &targetTup->dataBegin,
                 BF_NUM_ELEMENTS(sizeof(entry)));

        TRY_OR_RETURN(forcePage(pool, &leafNodePageHandle));
        TRY_OR_RETURN(unpinPage(pool, &leafNodePageHandle));

        return RC_OK;
    }

    // We don't have enough space and will need to split the leaf node
    //
    // We will need to turn the current node into a inner node
    // and copy the leaf entries into two separate nodes
    //

    IM_SplitLeafNodeCtx leafSplit = {};
    RC rc = IM_insertAndSplitLeafNode(
            leafNodePage,
            keyValue,
            &entry,
            entrySize,
            pool,
            maxEntriesPerNode,
            &leafSplit);
    if (rc != RC_OK) { PANIC("failed to split node: got rc = %d", rc); }

    // We then will need to link:
    //  * the new root entry pointer to the left node
    //  * the next page pointer to the right node

    // Reopen the right page
    BM_PageHandle rightPageHandle = {};
    TRY_OR_RETURN(pinPage(pool, &rightPageHandle, leafSplit.rightPageNum));
    RM_Page *rightPage = rightPage = (RM_Page *) rightPageHandle.buffer;

    // Clear all nodes on the old page and reset flags
    RM_page_deleteAllTuples(leafNodePage);

    // Get the key value of the first element in the right node
    const RM_PageSlotId referenceSlotIdx = 0;
    RM_PageTuple *referenceTup = RM_Page_getTuple(rightPage, referenceSlotIdx, NULL);

    IM_ENTRY_FORMAT_T referenceEntry;
    IM_readEntryI32(referenceTup, &referenceEntry, sizeof(referenceEntry));
    int32_t linkEntryKey = BF_AS_I32(referenceEntry.idxEntryKey);

    // Insert entry that links to the first element in the right node
    IM_ENTRY_FORMAT_T linkEntry = IM_ENTRY_FORMAT_OF_INT32;
    BF_SET_I32(linkEntry.idxEntryKey) = linkEntryKey;
    BF_SET_U16(linkEntry.idxEntryRidPage) = leafSplit.rightPageNum;
    BF_SET_U16(linkEntry.idxEntryRidSlot) = referenceSlotIdx;

    uint16_t linkEntrySize = BF_recomputeSize(
            (BF_MessageElement *) &linkEntry,
            BF_NUM_ELEMENTS(sizeof(linkEntry)));

    // Check if the parent inner node has enough space to insert the link tuple
    // to the right page. Otherwise, we're going to have to split the inner node
    // (ex)
    //  [1,3,4,5]  ==>    ~ 4 ~ (inserted into parent)
    //    n = 3           /   \
    //                 [1,3]  [5]
    //

    BM_PageHandle parentPageHandle = {};
    TRY_OR_RETURN(pinPage(pool, &parentPageHandle, parentPageNum));
    RM_Page *parentPage = (RM_Page *) parentPageHandle.buffer;

    if (parentPage->header.numTuples != maxEntriesPerNode) {
        // Parent has enough space to add link to the right leaf node
        uint16_t slotId = IM_getEntryInsertionIndex(linkEntryKey, parentPage, maxEntriesPerNode);
        RM_PageTuple *linkTup = RM_reserveTupleAtIndex(parentPage, linkEntrySize, slotId);

        // Write out the link tuple
        uint16_t wrote = BF_write(
                (BF_MessageElement *) &linkEntry,
                &linkTup->dataBegin,
                BF_NUM_ELEMENTS(sizeof(linkEntry)));
        if (wrote > linkTup->len) { PANIC("buffer overrun"); }
        return RC_OK;
    }

    // Parent inner node is full, we're going to need to split it and perform an inner node split
    NOT_IMPLEMENTED();

    return RC_OK;
}

static RM_PageNumber IM_getLeafNode(
        RM_PageNumber rootPageNum,
        int32_t searchKey,
        uint16_t maxEntriesPerNode,
        RM_PageNumber *parent_out)
{
    BM_BufferPool *pool = g_instance->recordManager->bufferPool;

    BM_PageHandle parentNodeHandle = {};
    RM_Page *parentNode = NULL;

    BM_PageHandle nodeHandle = {};
    RM_Page *node = NULL;

    if (pinPage(pool, &nodeHandle, rootPageNum) != RC_OK) {
        PANIC("failed to get node page '%d'", rootPageNum);
    }

    node = (RM_Page *) nodeHandle.buffer;
    if (node->header.kind != RM_PAGE_KIND_INDEX) {
        PANIC("attempted to read from wrong page kind. expected index page.");
    }

    do {
        // If the node is still a leaf node, then we don't have to go any further
        if (IS_FLAG_SET(node->header.flags, RM_PAGE_FLAGS_INDEX_LEAF)) {
            break;
        }

        // Otherwise, the current node must be an internal node, a RID pointer
        // will point to the child node.
        //
        // Find the index that is less than or equal to the search key
        //
        uint16_t slotId = IM_getEntryInsertionIndex(searchKey, node, maxEntriesPerNode);

        // Traverse to the newly selected node
        RM_PageTuple *nextTup = RM_Page_getTuple(node, slotId, NULL);

        // Determine where the next page is located
        IM_ENTRY_FORMAT_T entry;
        IM_readEntryI32(nextTup, &entry, sizeof(entry));
        RM_PageNumber nextPageNum = BF_AS_U16(entry.idxEntryRidPage);

        // Unpin previous parent, if available
        if (parentNode != NULL) {
            if (unpinPage(pool, &parentNodeHandle) != RC_OK) {
                PANIC("failed to unpin parent node page");
            }
            memset(&parentNodeHandle, 0, sizeof(parentNodeHandle));
            parentNode = NULL;
        }

        // Move current page to parent page
        parentNode = node;
        parentNodeHandle = nodeHandle;

        // Pin the next node page
        if (pinPage(pool, &nodeHandle, nextPageNum) != RC_OK) {
            PANIC("failed to get node page '%d'", nextPageNum);
        }
        node = (RM_Page *) nodeHandle.buffer;

    } while (true);

    if (parent_out != NULL) {
        if (parentNode != NULL) {
            *parent_out = parentNode->header.pageNum;
            if (unpinPage(pool, &parentNodeHandle) != RC_OK) {
                PANIC("failed to unpin parent node page");
            }
        }
        else {
            *parent_out = rootPageNum;
        }
    }

    RM_PageNumber nodePageNum = node->header.pageNum;
    if (unpinPage(pool, &nodeHandle) != RC_OK) {
        PANIC("failed to unpin node page");
    }

    return nodePageNum;
}

static uint16_t IM_getEntryInsertionIndex(
        const int32_t keyValue,
        RM_Page *page,
        uint16_t maxEntriesPerNode)
{
    uint16_t slotNum = 0;
    for (; slotNum < maxEntriesPerNode; slotNum++) {
        // get position of tuple
        size_t slot = slotNum * sizeof(RM_PageSlotPtr);
        RM_PageTuple *markTup = RM_Page_getTuple(page, slotNum, NULL);

        // read in entry data
        IM_ENTRY_FORMAT_T markEntry;
        IM_readEntryI32(markTup, &markEntry, sizeof(markEntry));

        // stop if the current key is greater than or equal to the search
        int32_t markKey = BF_AS_I32(markEntry.idxEntryKey);
        if (markKey > keyValue) {
            break;
        }
    }

    return slotNum;
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