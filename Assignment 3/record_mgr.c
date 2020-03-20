#include "dberror.h"
#include "expr.h"
#include "tables.h"
#include "buffer_mgr.h"
#include "storage_mgr.h"
#include "record_mgr.h"

// table and manager
RC initRecordManager (void *mgmtData){
	return -1;
}

RC shutdownRecordManager (){
	return -1;
}

RC createTable (char *name, Schema *schema){
	return -1;
}

RC openTable (RM_TableData *rel, char *name){
	return -1;
}

RC closeTable (RM_TableData *rel){
	return -1;
}

RC deleteTable (char *name){
	return -1;
}

int getNumTuples (RM_TableData *rel){
	return -1;
}

// handling records in a table
RC insertRecord (RM_TableData *rel, Record *record){
	return -1;
}

RC deleteRecord (RM_TableData *rel, RID id){
	return -1;
}

RC updateRecord (RM_TableData *rel, Record *record){
	return -1;
}

RC getRecord (RM_TableData *rel, RID id, Record *record){
	return -1;
}

// scans
RC startScan (RM_TableData *rel, RM_ScanHandle *scan, Expr *cond){
	return -1;
}

RC next (RM_ScanHandle *scan, Record *record){
	return -1;
}

RC closeScan (RM_ScanHandle *scan){
	return -1;
}

// dealing with schemas
int getRecordSize (Schema *schema){
	return -1;
}

Schema *createSchema (int numAttr, char **attrNames, DataType *dataTypes, int *typeLength, int keySize, int *keys){
	return NULL;
}

RC freeSchema (Schema *schema){
	return -1;
}

// dealing with records and attribute values
RC createRecord (Record **record, Schema *schema){
	return -1;
}

RC freeRecord (Record *record){
	return -1;
}

RC getAttr (Record *record, Schema *schema, int attrNum, Value **value){
	return -1;
}

RC setAttr (Record *record, Schema *schema, int attrNum, Value *value){
	return -1;
}
