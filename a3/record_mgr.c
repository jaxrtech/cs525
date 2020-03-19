#include "record_mgr.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "buffer_mgr.h"
#include "rm_page.h"
#include "rm_macros.h"

typedef struct RM_Metadata {
    BM_BufferPool *bufferPool;
} RM_Metadata;

static RM_Metadata *g_instance = NULL;

static const char *const RM_MAGIC_BUF = RM_DATABASE_MAGIC;

#define RM_DEFAULT_FILENAME "storage.db"
#define RM_DEFAULT_NUM_POOL_PAGES (512)
#define RM_DEFAULT_REPLACEMENT_STRATEGY (RS_LRU)

#define RM_PAGE_DBHEADER (0)
#define RM_PAGE_SCHEMA (1)

static RC RM_writeDatabaseHeader(BM_BufferPool *pool)
{
    BM_PageHandle pageHandle = {};
    TRY_OR_RETURN(pinPage(pool, &pageHandle, RM_PAGE_DBHEADER));

    RM_DatabaseHeader *header = (RM_DatabaseHeader *) pageHandle.buffer;
    memcpy(header->magic, RM_MAGIC_BUF, RM_DATABASE_MAGIC_LEN);

    header->pageSize = PAGE_SIZE;
    header->numPages = 2; // include this page and the schema page
    header->schemaPageNum = RM_PAGE_SCHEMA; // schema page is always on page number 1

    TRY_OR_RETURN(forcePage(pool, &pageHandle));
    TRY_OR_RETURN(unpinPage(pool, &pageHandle));

    return RC_OK;
}

static RC RM_writeSchemaPage(BM_BufferPool *pool)
{
    BM_PageHandle pageHandle = {};
    TRY_OR_RETURN(pinPage(pool, &pageHandle, RM_PAGE_SCHEMA));

    RM_Page_init(pageHandle.buffer, RM_PAGE_SCHEMA, RM_PAGE_KIND_SCHEMA);

    TRY_OR_RETURN(forcePage(pool, &pageHandle));
    TRY_OR_RETURN(unpinPage(pool, &pageHandle));

    return RC_OK;
}

RC initRecordManager (void *mgmtData IGNORE_UNUSED)
{
    RC rc;
    if (g_instance != NULL) {
        return RC_OK;
    }

    g_instance = malloc(sizeof(RM_Metadata));
    if (g_instance == NULL) {
        PANIC("malloc: failed to allocate record manager metadata");
    }

    BM_BufferPool *pool = malloc(sizeof(BM_BufferPool));
    rc = initBufferPool(
            pool,
            RM_DEFAULT_FILENAME,
            RM_DEFAULT_NUM_POOL_PAGES,
            RM_DEFAULT_REPLACEMENT_STRATEGY,
            NULL);
    if (rc != RC_OK) {
        goto error;
    }
    g_instance->bufferPool = pool;

    // Check if this is a new file
    BM_PageHandle page = {};
    if ((rc = pinPage(pool, &page, RM_PAGE_DBHEADER)) != RC_OK) {
        goto error;
    }

    // Check if the database has been already initialized by checking the magic bytes
    if (memcmp(page.buffer, RM_MAGIC_BUF, RM_DATABASE_MAGIC_LEN) != 0) {
        if ((rc = unpinPage(pool, &page)) != RC_OK) {
            goto error;
        }

        // Write the database header in the first page
        rc = RM_writeDatabaseHeader(pool);
        if (rc != RC_OK) {
            goto error;
        }

        // Create schema page
        rc = RM_writeSchemaPage(pool);
        if (rc != RC_OK) {
            goto error;
        }
    }
    else {
        if ((rc = unpinPage(pool, &page)) != RC_OK) {
            goto error;
        }
    }

    return RC_OK;

error:
    shutdownRecordManager();
    return rc;
}

RC shutdownRecordManager ()
{
    if (g_instance == NULL) {
        return RC_OK;
    }

    BM_BufferPool *pool = g_instance->bufferPool;
    if (pool != NULL) {
        shutdownBufferPool(pool);
        free(pool);
        g_instance->bufferPool = NULL;
    }

    free(g_instance);
    g_instance = NULL;
    return RC_OK;
}

#define RM_MAX_ATTR_NAME_LEN (UINT8_MAX)

RC createTable (char *name, Schema *schema)
{
    RC rc;

    uint64_t tableNameLength = strlen(name);
    if (tableNameLength <= 0 || tableNameLength > RM_MAX_ATTR_NAME_LEN) {
        return RC_RM_NAME_TOO_LONG;       
    }

    // Create a new schema tuple in the schema page and add a new data page
    // We must first calculate how much space we're going to need for the schema data
    
    /**
     * Schema on-disk format
     * 
     * /--sizeof(u8)-------\/--strlen(name) * sizeof(char)-\/--sizeof(u8)---\
     * +-------------------+--------------------------, ,--+---------------+
     * | tbl name len (u8) | table name str (k)       \ \  | num attr (u8) |
     * +-------------------+--------------------------' '--+---------------+
     * 
     * /-- sizeof(u8)--\/----------- numKeys * sizeof(u8) --------------\
     * +---------------+----------------+----------+--------------------+
     * | num keys (u8) | key idx_0 (u8) | ... (u8) | key idx_(n-1) (u8) |
     * +---------------+----------------+----------+--------------------+
     * 
     * /-- sizeof(u8)--\ /---- sizeof(u8) ----\/--sizeof(u8)--\/--strlen(name) * sizeof(char)-\
     * +----------------+~~~~~~~~~~~~~~~~~~~~~+---------------+--------------------------, ,--+
     * | attr type (u8) | type len (opt) (u8) | name len (u8) | name str (k)             \ \  |
     * +----------------+~~~~~~~~~~~~~~~~~~~~~+---------------+--------------------------' '--+
     *                       ^
     *                       `-- (only specified for string types)
     */
    uint16_t spaceRequired = 0;

    // table name
    spaceRequired += sizeof(uint8_t);
    spaceRequired += tableNameLength;

    // num attrs
    spaceRequired += sizeof(uint8_t);

    // num keys
    const int schemaNumKeys = schema->keySize;
    spaceRequired += sizeof(uint8_t);
    spaceRequired += sizeof(uint8_t) * schemaNumKeys;

    // attrs
    int numAttrs = schema->numAttr;
    uint8_t attrNameLen[numAttrs];
    for (int i = 0; i < numAttrs; i++) {
        spaceRequired += sizeof(uint8_t); // type
        if (schema->dataTypes[i] == DT_STRING) {
            spaceRequired += sizeof(uint8_t); // type len
        }

        // pre-calculate attr name length
        uint64_t nameLen = strlen(schema->attrNames[i]);
        if (nameLen > RM_MAX_ATTR_NAME_LEN) {
            return RC_RM_NAME_TOO_LONG;
        }
        attrNameLen[i] = (uint8_t) nameLen;

        spaceRequired += sizeof(uint8_t); // attr name len
        spaceRequired += nameLen;
    }

    // Open schema page and reserve the required amount of space
    BM_BufferPool *pool = g_instance->bufferPool;
    BM_PageHandle pageHandle = {};
    TRY_OR_RETURN(pinPage(pool, &pageHandle, RM_PAGE_SCHEMA));

    RM_Page *page = (RM_Page *) pageHandle.buffer;
    void *tupleBuffer = RM_Page_reserveTuple(page, spaceRequired);
    if (tupleBuffer == NULL) {
        rc = RC_RM_NO_MORE_TUPLES;
        goto finally;
    }

    //
    // Write out of the schema tuple data
    //

    // table name
    RM_BUF_WRITE_LSTRING(tupleBuffer, name, tableNameLength);

    // num attrs
    RM_BUF_WRITE(tupleBuffer, uint8_t, numAttrs);

    // num keys
    RM_BUF_WRITE(tupleBuffer, uint8_t, schemaNumKeys);
    for (int i = 0; i < schemaNumKeys; i++) {
        RM_BUF_WRITE(tupleBuffer, uint8_t, schema->keyAttrs[i]);
    }

    // attrs
    for (int i = 0; i < numAttrs; i++) {
        RM_BUF_WRITE(tupleBuffer, uint8_t, schema->dataTypes[i]);
        if (schema->dataTypes[i] == DT_STRING) {
            RM_BUF_WRITE(tupleBuffer, uint8_t, schema->typeLength[i]);
        }
        RM_BUF_WRITE_LSTRING(tupleBuffer, schema->attrNames[i], attrNameLen[i]);
    }

    if ((rc = forcePage(pool, &pageHandle)) != RC_OK) {
        goto finally;
    }

    rc = RC_OK;

finally:
    TRY_OR_RETURN(unpinPage(pool, &pageHandle));
    return rc;
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
