#include <errno.h> 
#include "record_mgr.h"
#include "storage_mgr.h"
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
        forceShutdownBufferPool(pool);
        free(pool);
        g_instance->bufferPool = NULL;
    }

    free(g_instance);
    g_instance = NULL;

    //delete the pagefile
<<<<<<< HEAD
    if (destroyPageFile(RM_DEFAULT_FILENAME) != RC_OK) {
        return RC_FILE_DESTROY_ERROR; 
    }

=======
    
>>>>>>> 572be1fe16639c1f800ee4f109e1d832dabfbe4e
    return RC_OK;
}

#define RM_MAX_ATTR_NAME_LEN (UINT8_MAX)

RC createTable (char *name, Schema *schema)
{
    RC rc;

    //check if table with same name already exists
    RM_TableData *temp = (RM_TableData *) malloc(sizeof(RM_TableData));
    if ((openTable(temp, name)) == RC_OK){
        closeTable(temp);
        return RC_IM_KEY_ALREADY_EXISTS;
    }
    free(temp);

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
    meta->fileHandle->totalNumPages++;

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
    RM_PageTuple *tup = RM_Page_reserveTuple(page, spaceRequired);
    if (tup == NULL) {
        rc = RC_RM_NO_MORE_TUPLES;
        goto finally;
    }

    //
    // Write out of the schema tuple data
    //
    void *tupleBuffer = &tup->dataBegin;
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
    int i=0;
    while (i<num) {
        size_t slot = i * sizeof(RM_PageSlotPtr);
        off = (RM_PageSlotPtr *) (&pg->dataBegin + slot);
        printf("openTable: [tup#%d] pg = %p, pg->data = %p, slot_off = %d, off = %d\n", i, pg, &pg->dataBegin, slot, *off);
        fflush(stdout);
        tup = (RM_PageTuple *) (&pg->dataBegin + *off);

        schemaMsg = RM_SCHEMA_FORMAT;
        BF_read((BF_MessageElement *) &schemaMsg, &tup->dataBegin, BF_NUM_ELEMENTS(sizeof(schemaMsg)));
        if (strncmp(BF_AS_STR(schemaMsg.tblName), name, BF_STRLEN(schemaMsg.tblName)) == 0) {
            hit = true;
            break;
        }
        i++;
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
    //write all pages back to disk
    BM_BufferPool *pool = g_instance->bufferPool;
    forceFlushPool(pool);

    //free schema then relation
    free(rel->schema->attrNames);
    free(rel->schema->dataTypes);
    free(rel->schema->typeLength);
    free(rel->schema->keyAttrs);
    free(rel->schema);

    // IMPORTANT: caller is in-charge of freeing the structure itself, if necessary

    return RC_OK;
}

/* close a table and mark pages as empty */

RC deleteTable (char *name)
{
    BM_BufferPool *pool = g_instance->bufferPool;
    forceFlushPool(pool); //write all pages to disk just in case

    //open temporary relation in memory so we can scan its pages
    RM_TableData *rel = (RM_TableData *) malloc(sizeof(RM_TableData));
    openTable(rel, name);
    
    int pageNum = rel->schema->dataPageNum;
    BM_PageHandle handle;

    //mark pages as empty in relation
    do{
        if (pinPage(pool, &handle, pageNum) != RC_OK) { return -1; }
            RM_Page *page = (RM_Page *) handle.buffer;
            pageNum = page->header.nextPageNum;

            //NOT_IMPLEMENTED(); //mark page as empty here

        if (unpinPage(pool, &handle) != RC_OK) { return -1; }

    } while ( pageNum != -1 ); //will quit when there isn't a new page delete

    closeTable(rel); //frees rel 

    //delete tuple in schema page
    TRY_OR_RETURN(pinPage(pool, &handle, RM_PAGE_SCHEMA));
    RM_Page *pg = (RM_Page *) handle.buffer;
    RM_PageHeader *hdr = &pg->header;

    //get the record id for the schema tuple
    RID *rid = (RID *) malloc(sizeof(RID));
    rid->page = RM_PAGE_SCHEMA;

    //get the slot number for the relation tuple
    RM_PageTuple *tup;
    RM_PageSlotPtr *off;
    uint16_t num = hdr->numTuples;
    struct RM_SCHEMA_FORMAT_T schemaMsg;
    int i=0;
    while (i<num) {
        size_t slot = i * sizeof(RM_PageSlotPtr);
        off = (RM_PageSlotPtr *) (&pg->dataBegin + slot);
        fflush(stdout);
        tup = (RM_PageTuple *) (&pg->dataBegin + *off);

        schemaMsg = RM_SCHEMA_FORMAT;
        BF_read((BF_MessageElement *) &schemaMsg, &tup->dataBegin, BF_NUM_ELEMENTS(sizeof(schemaMsg)));
        if (strncmp(BF_AS_STR(schemaMsg.tblName), name, BF_STRLEN(schemaMsg.tblName)) == 0) {
            rid->slot = i; //mark RID slot
            break;
        }
    }

    // Null the schema tuple data
    RM_Page_deleteTuple(pg, *rid);

    TRY_OR_RETURN(forcePage(pool, &handle)); ////Note: FOR SOME REASON it also re-inits the DB
    TRY_OR_RETURN(unpinPage(pool, &handle));
}

int getNumTuples (RM_TableData *rel)
{
    int totalNumTups = 0;

    int pageNum = rel->schema->dataPageNum;
    BM_BufferPool *pool = g_instance->bufferPool;
    BM_PageHandle handle;

    do{
        if (pinPage(pool, &handle, pageNum) != RC_OK) { return -1; }

        RM_Page *page = (RM_Page *) handle.buffer;
        RM_PageHeader *header = &page->header;

        uint16_t numTups = header->numTuples;
        totalNumTups += (int) numTups;
        pageNum = header->nextPageNum;

        if (unpinPage(pool, &handle) != RC_OK) { return -1; }

    } while ( pageNum != -1 ); //will quit when there isn't a new page to scan

    return totalNumTups;
}

// handling records in a table
RC insertRecord (RM_TableData *rel, Record *record)
{
    //get page that holds the records for the data. Assume overflow handled in RM_ReserveTuple(...);
    int pageNum = rel->schema->dataPageNum;
    BM_BufferPool *pool = g_instance->bufferPool;
    BP_Metadata *meta = pool->mgmtData;

    BM_PageHandle pageHandle = {};
    size_t recordSize = getRecordSize(rel->schema);

    //loop through all available pages in relation
    do {
        TRY_OR_RETURN(pinPage(pool, &pageHandle, pageNum));

        RM_Page *page = (RM_Page *) pageHandle.buffer;
        RM_PageHeader *pageHeader = &page->header;

        RM_PageTuple *tup = RM_Page_reserveTuple(page, recordSize);
        if (tup == NULL) {
            unpinPage(pool, &pageHandle);
            //check overflow pages. if none, create and link them
            if (pageHeader->nextPageNum != -1) {
                pageNum = (int) pageHeader->nextPageNum;
                continue; //try with next page
            } else {
                int newpageNum = meta->fileHandle->totalNumPages;
                meta->fileHandle->totalNumPages++;

                //create new page
                BM_PageHandle newdata = {};
                TRY_OR_RETURN(pinPage(pool, &newdata, newpageNum));
                RM_Page_init(newdata.buffer, newpageNum, RM_PAGE_KIND_DATA);
                TRY_OR_RETURN(forcePage(pool, &newdata));
                TRY_OR_RETURN(unpinPage(pool, &newdata));
                //link new page to table
                pageHeader->nextPageNum = newpageNum;

                //set page to reserve tuple and try again
                pageNum = newpageNum;
                continue;
            }
            PANIC("cannot create a new page to insert record");
        }

        record->id.page = pageNum;
        record->id.slot = tup->slotId;
        memcpy(&tup->dataBegin, record->data, recordSize);

        TRY_OR_RETURN(markDirty(pool, &pageHandle));
        TRY_OR_RETURN(unpinPage(pool, &pageHandle));

        printf("insertRecord: table = \"%s\", rid = %d:%d\n", rel->name, record->id.page, record->id.slot);
        return RC_OK;

    } while (1); // hdr->nextPageNum != -1 );
}

/* Finds the record using an RID and deletes it from table. 
 * Also sets flags for additional slot ptrs
 * 
 */
RC deleteRecord (RM_TableData *rel, RID id)
{


    BM_BufferPool *pool = g_instance->bufferPool;
    BM_PageHandle handle;

    //pin page containing record if it exists
    TRY_OR_RETURN(pinPage(pool, &handle, id.page)); //page nonexistent or RC_OK
    RM_Page *page = (RM_Page *) handle.buffer;

    RM_Page_deleteTuple(page, id);

    TRY_OR_RETURN(markDirty(pool, &handle));
    TRY_OR_RETURN(unpinPage(pool, &handle));
    return RC_OK;
}

//take a record that exists in the table and update it
RC updateRecord (RM_TableData *rel, Record *record)
{
    BM_BufferPool *pool = g_instance->bufferPool;
    BM_PageHandle handle;

    //pin page containing record if it exists
    TRY_OR_RETURN(pinPage(pool, &handle, record->id.page)); //page nonexistent or RC_OK
    RM_Page *page = (RM_Page *) handle.buffer;

    RM_Page_setTuple(page, record);

    TRY_OR_RETURN(markDirty(pool, &handle));
    TRY_OR_RETURN(unpinPage(pool, &handle));
    return RC_OK;
}

RC getRecord (RM_TableData *rel, RID id, Record *record) //assume RID points to any page (even overflow pages)
{
    BM_BufferPool *pool = g_instance->bufferPool;
    BM_PageHandle handle;

    //pin page containing record if it exists
    TRY_OR_RETURN(pinPage(pool, &handle, id.page)); //page nonexistent or RC_OK
    RM_Page *page = (RM_Page *) handle.buffer;

    RM_Page_getTuple(page, record, id);
    unpinPage(pool, &handle);
    return RC_OK;
}

// scans
/* Starting a scan initializes the RM_ScanHandle data structure passed as an argument to startScan */
RC startScan (RM_TableData *rel, RM_ScanHandle *scan, Expr *cond)
{
    scan->rel = rel;
    scan->mgmtData = (void*)cond;
    return RC_OK;
}

//NOTE: if cond is NULL, then we will get all tuples
RC next (RM_ScanHandle *scan, Record *record)
{
    //unpack
    Expr *cond = (Expr *)(scan->mgmtData);
    RM_TableData *rel = scan->rel;

    //try to gettuple THEN check if it works with expr

}

/* Closing a scan indicates to the record manager that all associated resources can be cleaned up. */
RC closeScan (RM_ScanHandle *scan)
{
    //unpin pages?
    NOT_IMPLEMENTED();
    return RC_OK;
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
                PANIC("unhandled datatype: dt = %d", dt);
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
            result->v.stringV = (char *) malloc(strlen(buf)+1);
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
        case DT_STRING: 
            ;//<--deleting this raises error due to next line
            int len = schema->typeLength[attrNum];      //get str length
            strncpy((char*)buf, value->v.stringV, len); //copy string from Value
            buf += offset;                              //increment buffer by offset for next
            break;

        case DT_BOOL:
            *(bool *) buf = value->v.boolV;  //set value to buf
            buf += sizeof(bool);             //move buf ptr
            break;

        case DT_FLOAT:
            *(float *) buf = value->v.floatV; //set value to buf
            buf += sizeof(float);             //move buf ptr
            break;

        case DT_INT:
            *(int *) buf = value->v.intV;   //set value to buf
            buf += sizeof(int);             //move buf ptr
            break;

        default:
            PANIC("Setting Unknown Attribute");
            exit(1);
    }

    return RC_OK;
}
