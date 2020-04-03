#include "record_mgr.h"
//#include "tables.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "buffer_mgr.h"
#include "rm_page.h"
#include "rm_macros.h"
#include "rm_binfmt.h"

typedef struct RM_Metadata {
    BM_BufferPool *bufferPool;
} RM_Metadata;

static RM_Metadata *g_instance = NULL;

static const char *const RM_MAGIC_BUF = RM_DATABASE_MAGIC;

#define RM_DEFAULT_FILENAME "storage.db"
#define RM_DEFAULT_NUM_POOL_PAGES (512)
#define RM_DEFAULT_REPLACEMENT_STRATEGY (RS_LRU)

//special pagenumbers
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
    printf("ASSIGNMENT 3 (Storage Manager)\n\tCS 525 - SPRING 2020\n\tCHRISTOPHER MORCOM & JOSH BOWDEN\n\n");
    
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

    //
    // Initialize new data page
    //
    BM_BufferPool *pool = g_instance->bufferPool;
    BP_Metadata *meta = pool->mgmtData;
    int dataPageNum = meta->fileHandle->totalNumPages;

    BM_PageHandle dataPageHandle = {};
    TRY_OR_RETURN(pinPage(pool, &dataPageHandle, dataPageNum));
    RM_Page_init(dataPageHandle.buffer, dataPageNum, RM_PAGE_KIND_DATA);
    TRY_OR_RETURN(forcePage(pool, &dataPageHandle));
    TRY_OR_RETURN(unpinPage(pool, &dataPageHandle));

    //
    // Setup/copy information from `Schema` interface into the disk format
    //
    int numColumns = schema->numAttr;
    size_t attrsSizeBytes = numColumns * sizeof(RM_SCHEMA_ATTR_FORMAT);
    struct RM_SCHEMA_ATTR_FORMAT_T *attrs = alloca(attrsSizeBytes);
    for (int i = 0; i < numColumns; i++) {
        if (strlen(schema->attrNames[i]) > RM_MAX_ATTR_NAME_LEN) {
            return RC_RM_NAME_TOO_LONG;
        }
        
        attrs[i] = RM_SCHEMA_ATTR_FORMAT;
        BF_SET_U8 (attrs[i].attrType) = (uint8_t) schema->dataTypes[i];
        BF_SET_STR(attrs[i].attrName) = schema->attrNames[i];
        BF_SET_U8 (attrs[i].attrTypeLen) = schema->typeLength[i];
    }

    struct RM_SCHEMA_FORMAT_T schemaDisk = RM_SCHEMA_FORMAT;
    BF_SET_U16(schemaDisk.tblDataPageNum) = dataPageNum;
    BF_SET_STR(schemaDisk.tblName) = name;
    BF_SET_U8(schemaDisk.tblNumAttr) = numColumns;
    BF_SET_ARRAY_MSG(schemaDisk.tblAttrs, attrs, attrsSizeBytes);

    int numKeys = schema->keySize;
    size_t keysDiskSize = numKeys * sizeof(uint8_t);
    uint8_t *keysDisk = alloca(keysDiskSize);
    for (int i = 0; i < numKeys; i++) {
        keysDisk[i] = (uint8_t) schema->keyAttrs[i];
    }
    BF_SET_ARRAY_U8(schemaDisk.tblKeys, keysDisk, keysDiskSize);
    uint16_t spaceRequired = BF_recomputeSize(
            (BF_MessageElement *) &schemaDisk,
            BF_NUM_ELEMENTS(sizeof(schemaDisk)));

    //
    // Open schema page and reserve the required amount of space
    //
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
    BF_write((BF_MessageElement *) &schemaDisk, tupleBuffer, BF_NUM_ELEMENTS(sizeof(schemaDisk)));
    if ((rc = forcePage(pool, &pageHandle)) != RC_OK) {
        goto finally;
    }

    rc = RC_OK;

    finally:
        TRY_OR_RETURN(unpinPage(pool, &pageHandle));
        return rc;
}

//move a table to memory by reading it from disk
RC openTable (RM_TableData *rel, char *name)
{
    //set up page reading
    BM_BufferPool *pool = g_instance->bufferPool;
    BM_PageHandle pageHandle = {};
    //store the schema page in pageHandle
    TRY_OR_RETURN(pinPage(pool, &pageHandle, RM_PAGE_SCHEMA));
    
    //open the schema page and store the header and data
    RM_Page *pg = (RM_Page*) pageHandle.buffer;
    RM_PageHeader *hdr = &pg->header;
    //as long as this isn't zero, it is an offset from the header to the tuple
    //increment it by 2 bytes to get the next pointer
    RM_PageSlotPtr *off; //deref this address for the offset

    //find the pointer in the data that points to the proper table tuple (or error if table doesn't exist)
    RM_PageTuple *tup;
    uint16_t num = hdr->numTuples;
    bool hit = false;
    struct RM_SCHEMA_FORMAT_T schemaMsg;
    for (int i = 0; i < num; i++) {
        off = (RM_PageSlotPtr *) (pg->data + (i * sizeof(RM_PageSlotPtr)));
        tup = (RM_PageTuple *) (pg->data + *off);

        schemaMsg = RM_SCHEMA_FORMAT;
        BF_read((BF_MessageElement *) &schemaMsg, tup, BF_NUM_ELEMENTS(sizeof(schemaMsg)));
        if (strncmp(BF_AS_STR(schemaMsg.tblName), name, BF_STRLEN(schemaMsg.tblName)) == 0) {
            hit = true;
            break;
        }
    }

    if (!hit) {
        return RC_RM_UNKNOWN_TABLE;
    }

    int numKeys = BF_ARRAY_U8_LEN(schemaMsg.tblKeys);
    int numAttrs = BF_AS_U8(schemaMsg.tblNumAttr);
    rel->name = BF_AS_STR(schemaMsg.tblName);
    rel->schema = malloc(sizeof(struct Schema));
    rel->schema->dataPageNum = BF_AS_U16(schemaMsg.tblDataPageNum);
    rel->schema->numAttr = numAttrs;
    rel->schema->keySize = numKeys;
    rel->schema->keyAttrs = calloc(numKeys, sizeof(int));
    uint8_t *keyIndexes = BF_AS_ARRAY_U8(schemaMsg.tblKeys);
    for (int i = 0; i < numKeys; i++) {
        rel->schema->keyAttrs[i] = keyIndexes[i];
    }
    rel->schema->dataTypes = calloc(numAttrs, sizeof(enum DataType));
    rel->schema->attrNames = calloc(numAttrs, sizeof(char *));
    rel->schema->typeLength = calloc(numAttrs, sizeof(int));
    struct RM_SCHEMA_ATTR_FORMAT_T *dataTypes = (struct RM_SCHEMA_ATTR_FORMAT_T *) BF_AS_ARRAY_MSG(schemaMsg.tblAttrs);
    for (int i = 0; i < numAttrs; i++) {
        struct RM_SCHEMA_ATTR_FORMAT_T *attr = &dataTypes[i];
        rel->schema->dataTypes[i] = BF_AS_U8(attr->attrType);
        rel->schema->typeLength[i] = BF_AS_U8(attr->attrTypeLen);
        rel->schema->attrNames[i] = BF_AS_STR(attr->attrName);
    }
    
    return RC_OK;
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
    int pageNum = rel->schema->dataPageNum;
    BM_BufferPool *pool = g_instance->bufferPool;

    BM_PageHandle handle;
    if (pinPage(pool, &handle, pageNum) != RC_OK) {
        return -1;
    }

    RM_Page *page = (RM_Page *) handle.buffer;
    uint16_t numTups = page->header.numTuples;
    if (unpinPage(pool, &handle) != RC_OK) {
        return -1;
    }

    return numTups;
}

// handling records in a table
RC insertRecord (RM_TableData *rel, Record *record)
{
    int pageNum = rel->schema->dataPageNum;
    BM_BufferPool *pool = g_instance->bufferPool;

    BM_PageHandle handle;
    TRY_OR_RETURN(pinPage(pool, &handle, pageNum));

    RM_Page *page = (RM_Page *) handle.buffer;
    size_t size = getRecordSize(rel->schema);
    void *tup = RM_Page_reserveTuple(page, size);
    memcpy(tup, record->data, size);

    TRY_OR_RETURN(markDirty(pool, &handle));
    TRY_OR_RETURN(unpinPage(pool, &handle));

    return RC_OK;
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
            default:
                PANIC("unhandled datatype");
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
    int size = getRecordSize(schema);
    Record *r = malloc(sizeof(Record) + size);
    if (r == NULL) {
        PANIC("out of memory");
        return RC_WRITE_FAILED;
    }

    r->data = (char *) r + sizeof(Record);
    *record = r;
    return RC_OK;
}

RC freeRecord (Record *record)
{
    free(record);
    return RC_OK;
}

RC getAttr (Record *record, Schema *schema, int attrNum, Value **value)
{
    if (attrNum < 0 || attrNum >= schema->numAttr) {
        return RC_RM_ATTR_NUM_OUT_OF_BOUNDS;
    }

    int offset;
    TRY_OR_RETURN(getAttrOffset(schema, attrNum, &offset));

    Value *result = malloc(sizeof(Value));
    if (result == NULL) {
        PANIC("out of memory");
        exit(1);
    }

    DataType dt = schema->dataTypes[attrNum];
    result->dt = dt;
    void *buf = record->data + offset;
    switch (dt) {
        case DT_STRING:
            result->v.stringV = (char *) malloc(strlen(buf) + 1);
            strcpy(result->v.stringV, buf);
            break;

        case DT_BOOL:
            result->v.boolV = *(bool *) buf;
            break;

        case DT_FLOAT:
            result->v.floatV = *(float *) buf;
            break;

        case DT_INT:
            result->v.intV = *(int *) buf;
            break;

        default: PANIC("unhandled datatype");
    }

    *value = result;
    return RC_OK;
}

//using the above as a reference, set (union) value to Schema
RC setAttr (Record *record, Schema *schema, int attrNum, Value *value) 
{
    if (attrNum < 0 || attrNum >= schema->numAttr) {
        return RC_RM_ATTR_NUM_OUT_OF_BOUNDS;
    }

    int offset;
    TRY_OR_RETURN(getAttrOffset(schema, attrNum, &offset));

    Value *result = malloc(sizeof(Value));
    if (result == NULL) {
        PANIC("out of memory");
        exit(1);
    }

    DataType dt = schema->dataTypes[attrNum];
    result->dt = dt;
    void *buf = record->data + offset; //remember void ptr is 8 Bytes in length 
    switch (dt) {
        case DT_STRING: ;//<--deleting this raises label can't be part of statement error?
            int len = schema->typeLength[attrNum];      //get str length
            strncpy((char*)buf, value->v.stringV, len); //copy string from Value
            buf += offset;                              //increment buffer by offset for next
            break;

        case DT_BOOL:
            break;

        case DT_FLOAT:
            break;

        case DT_INT:
            *(int *) buf = value->v.intV;   //set value to buf
            buf += sizeof(int);             //move buf ptr
            break;

        default:
            
            break;
    }

    return RC_OK;
}
