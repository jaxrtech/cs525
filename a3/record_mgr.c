#include "record_mgr.h"
#include <stdio.h>
#include <stdlib.h>

#define NOT_IMPLEMENTED() \
    do { \
        fprintf(stderr, "NOT IMPLEMENTED: %s(%s)\n", __FILE__, __FUNCTION__); \
        exit(1); \
    } while (0)

RC initRecordManager (void *mgmtData)
{
    NOT_IMPLEMENTED();
}

RC shutdownRecordManager ()
{
    NOT_IMPLEMENTED();
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
    NOT_IMPLEMENTED();
}

Schema *createSchema (
        int numAttr,
        char **attrNames,
        DataType *dataTypes,
        int *typeLength,
        int keySize,
        int *keys)
{
    NOT_IMPLEMENTED();
}

RC freeSchema (Schema *schema)
{
    NOT_IMPLEMENTED();
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
