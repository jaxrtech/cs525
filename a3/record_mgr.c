#include "record_mgr.h"

#include <stdio.h>
#include <stdlib.h>

#include "buffer_mgr.h"

#define NOT_IMPLEMENTED() \
    do { \
        fprintf(stderr, "NOT IMPLEMENTED: %s(%s)\n", __FILE__, __FUNCTION__); \
        exit(1); \
    } while (0)

#define PANIC(msg, vargs...) \
    do { \
        fprintf(stderr, "panic! [%s] %s:L%d: " msg "\n", \
                __FILE__,  \
                __FUNCTION__, \
                __LINE__, \
                ##vargs); \
        fflush(stderr); \
        exit(1); \
    } while (0)

#define IGNORE_UNUSED __attribute__((unused))

typedef struct RM_Metadata {
    BM_BufferPool *bufferPool;
} RM_Metadata;

static RM_Metadata *g_instance = NULL;

#define RM_DEFAULT_FILENAME "storage.db"
#define RM_DEFAULT_NUM_POOL_PAGES (512)
#define RM_DEFAULT_REPLACEMENT_STRATEGY (RS_LRU)

static RC RM_ensureRootPage(BM_BufferPool *pool, PageNumber pageNum)
{
    
}

RC initRecordManager (void *mgmtData IGNORE_UNUSED)
{
    RC rc;
    if (g_instance != NULL) {
        return RC_OK;
    }

    g_instance = malloc(sizeof(RM_Metadata));
    if (g_instance) {
        return -1;
    }

    BM_BufferPool *pool = MAKE_POOL();
    g_instance->bufferPool = pool;
    rc = initBufferPool(
            pool,
            RM_DEFAULT_FILENAME,
            RM_DEFAULT_NUM_POOL_PAGES,
            RM_DEFAULT_REPLACEMENT_STRATEGY,
            NULL);
    if (rc != RC_OK) {
        free(g_instance);
        return rc;
    }

    // Ensure that the first meta-data page is created
    BM_PageHandle metaPage = {};
    rc = pinPage(pool, &metaPage, 0);
    if (rc != RC_OK) {
        shutdownBufferPool(pool);
        free(g_instance);
        return rc;
    }

    RM_ensureRootPage(pool, 0);

    return RC_OK;
}

RC shutdownRecordManager ()
{
    if (g_instance == NULL) {
        return RC_OK;
    }

    free(g_instance);
    return RC_OK;
}

RC createTable (char *name, Schema *schema)
{
    NOT_IMPLEMENTED();
}

RC openTable (RM_TableData *rel, char *name)
{
    NOT_IMPLEMENTED();
}

RC closeTable (RM_TableData *rel)
{
    NOT_IMPLEMENTED();
}

RC deleteTable (char *name)
{
    NOT_IMPLEMENTED();
}

int getNumTuples (RM_TableData *rel)
{
    NOT_IMPLEMENTED();
}

// handling records in a table
RC insertRecord (RM_TableData *rel, Record *record)
{
    NOT_IMPLEMENTED();
}

RC deleteRecord (RM_TableData *rel, RID id)
{
    NOT_IMPLEMENTED();
}

RC updateRecord (RM_TableData *rel, Record *record)
{
    NOT_IMPLEMENTED();
}

RC getRecord (RM_TableData *rel, RID id, Record *record)
{
    NOT_IMPLEMENTED();
}

// scans
RC startScan (RM_TableData *rel, RM_ScanHandle *scan, Expr *cond)
{
    NOT_IMPLEMENTED();
}

RC next (RM_ScanHandle *scan, Record *record)
{
    NOT_IMPLEMENTED();
}

RC closeScan (RM_ScanHandle *scan)
{
    NOT_IMPLEMENTED();
}

// dealing with schemas
int getRecordSize (Schema *schema)
{
    if (schema == NULL) {
        PANIC("`schema` cannot be null");
    }

    int total = 0;
    int n = schema->numAttr;
    for (int i = 0; i < n; i++) {
        DataType dt = schema->dataTypes[i];
        switch (dt) {
            case DT_INT: total += sizeof(int); break;
            case DT_BOOL: total += sizeof(bool); break;
            case DT_FLOAT: total += sizeof(float); break;
            case DT_STRING: total += schema->typeLength[i]; break;
            default: PANIC("unhandled datatype");
        }
    }

    return total;
}

Schema *createSchema (
        int numAttr,
        char **attrNames,
        DataType *dataTypes,
        int *typeLength,
        int keySize,
        int *keys)
{
    Schema *self = malloc(sizeof(Schema));
    self->numAttr = numAttr;
    self->attrNames = attrNames;
    self->dataTypes = dataTypes;
    self->typeLength = typeLength;
    self->keySize = keySize;
    self->keyAttrs = keys;
    return self;
}

RC freeSchema (Schema *schema)
{
    free(schema);
    return RC_OK;
}

// dealing with records and attribute values
RC createRecord (Record **record, Schema *schema)
{
    NOT_IMPLEMENTED();
}

RC freeRecord (Record *record)
{
    NOT_IMPLEMENTED();
}

RC getAttr (Record *record, Schema *schema, int attrNum, Value **value)
{
    NOT_IMPLEMENTED();
}

RC setAttr (Record *record, Schema *schema, int attrNum, Value *value)
{
    NOT_IMPLEMENTED();
}
