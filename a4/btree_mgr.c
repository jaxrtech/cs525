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

static IM_Metadata *g_instance = NULL;

// called by record manager -- do not mark as `static`
RC IM_writeIndexPage(BM_BufferPool *pool)
{
    BM_PageHandle pageHandle = {};
    TRY_OR_RETURN(pinPage(pool, &pageHandle, RM_PAGE_SCHEMA));

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

    printf("%d\n", dataPageNum);
    BM_PageHandle dataPageHandle = {};
    TRY_OR_RETURN(pinPage(pool, &dataPageHandle, dataPageNum));
    RM_Page_init(dataPageHandle.buffer, dataPageNum, RM_PAGE_KIND_INDEX);
    TRY_OR_RETURN(forcePage(pool, &dataPageHandle));
    TRY_OR_RETURN(unpinPage(pool, &dataPageHandle));

    //
    // Setup information for the index descriptor into the disk format
    //
    struct IM_DESCRIPTOR_FORMAT_T indexDisk = IM_DESCRIPTOR_FORMAT;
    BF_SET_STR(indexDisk.idxName) = idxId;
    BF_SET_U8(indexDisk.idxKeyType) = keyType;
    BF_SET_U16(indexDisk.idxMaxEntriesPerNode) = n;

    uint16_t spaceRequired = BF_recomputeSize(
            (BF_MessageElement *) &indexDisk,
            BF_NUM_ELEMENTS(sizeof(indexDisk)));

    //
    // Open index descriptor page and reserve the required amount of space
    //
    BM_PageHandle pageHandle = {};
    TRY_OR_RETURN(pinPage(pool, &pageHandle, RM_PAGE_INDEX));

    RM_Page *page = (RM_Page *) pageHandle.buffer;
    RM_PageTuple *tup = RM_Page_reserveTuple(page, spaceRequired);
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
    RC rc;

    struct IM_DESCRIPTOR_FORMAT_T indexMsg = {};
    BM_PageHandle pageHandle = {};
    if ((rc = IM_findIndex(idxId, &pageHandle, NULL, &indexMsg)) != RC_OK) {
        return rc;
    }

    BTreeHandle *indexHandle = malloc(sizeof(BTreeHandle));
    indexHandle->idxId = idxId;
    indexHandle->keyType = BF_AS_U8(indexMsg.idxKeyType);

    //store the data of the first page for the tree 
    indexHandle->mgmtData = NULL; // TODO

    if (tree != NULL) {
        *tree = indexHandle;
    }

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

RC binarySearchNode(Node *node, Value *key, RID *result, int NodeType){
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


RC insertKey (BTreeHandle *tree, Value *key, RID rid){
    //inserts a new key/recordptr into the tree (NO DUPLICATES) 
    // returns RC_OK or RC_IM_KEY_ALREADY_EXISTS
	NOT_IMPLEMENTED();
}
RC deleteKey (BTreeHandle *tree, Value *key){
    //delete key and record pointer from tree and rebalance or RC_IM_KEY_NOT_FOUND
    NOT_IMPLEMENTED();
}
RC openTreeScan (BTreeHandle *tree, BT_ScanHandle **handle){
    handle = malloc(sizeof BT_ScanHandle);
	handle->tree = tree;

    BT_ScanData *scandata = malloc(sizeof BT_ScanData);
    scandata->currentNode = getLeafNodePtr(tree, NULL); //get leftmost leaf node
    scandata->nodeIdx = 0;
    handle->mgmtData = scandata;

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