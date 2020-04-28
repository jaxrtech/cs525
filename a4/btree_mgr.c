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
//#include "rm_binfmt.h"

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
	NOT_IMPLEMENTED();
}

RC openBtree (BTreeHandle **tree, char *idxId)
{
	NOT_IMPLEMENTED();
}

RC closeBtree (BTreeHandle *tree)
{
	NOT_IMPLEMENTED();
}

RC deleteBtree (char *idxId){
	NOT_IMPLEMENTED();
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

// index access
RC findKey (BTreeHandle *tree, Value *key, RID *result){
	NOT_IMPLEMENTED();
}
RC insertKey (BTreeHandle *tree, Value *key, RID rid){
	NOT_IMPLEMENTED();
}
RC deleteKey (BTreeHandle *tree, Value *key){
	NOT_IMPLEMENTED();
}
RC openTreeScan (BTreeHandle *tree, BT_ScanHandle **handle){
	NOT_IMPLEMENTED();
}
RC nextEntry (BT_ScanHandle *handle, RID *result){
	NOT_IMPLEMENTED();
}
RC closeTreeScan (BT_ScanHandle *handle){
	NOT_IMPLEMENTED();
}

// debug and test functions
char *printTree (BTreeHandle *tree){
	NOT_IMPLEMENTED();
}