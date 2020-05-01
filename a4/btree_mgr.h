#pragma once
#ifndef BTREE_MGR_H
#define BTREE_MGR_H

#include <stdio.h>
#include <stdlib.h>
#include <stdarg.h>

#include "dberror.h"
#include "tables.h"
#include "buffer_mgr.h"

//define type flags for internal and leaf nodes
#define LEAF_NODE 0
#define INTERNAL_NODE 1

// structure for accessing btrees
typedef struct BTreeHandle {
  DataType keyType;
  char *idxId;
  void *mgmtData;
} BTreeHandle;

typedef struct Node {
	struct Node *parent; //points to parent in case we have to split or merge nodes
	int fill;	//number of KEYS in node (check against N)
	void *keys; //array of keys
	void *ptrs;	//array of pointers for each key (ptrs[fill+1] is the pointer to next node)
				//ptrs are RIDs
} Node;

typedef struct BT_ScanData {
	Node *currentNode; //store the current leaf node
	int nodeIdx;	   //stores the last index in the node checked.
} BT_ScanData;

typedef struct BT_ScanHandle {
  BTreeHandle *tree;
  BT_ScanData *mgmtData;
} BT_ScanHandle;

// init and shutdown index manager
extern RC initIndexManager (void *mgmtData);
extern RC shutdownIndexManager ();

// create, destroy, open, and close an btree index
extern RC createBtree (char *idxId, DataType keyType, int n);
extern RC openBtree (BTreeHandle **tree, char *idxId);
extern RC closeBtree (BTreeHandle *tree);
extern RC deleteBtree (char *idxId);

// access information about a b-tree
extern RC getNumNodes (BTreeHandle *tree, int *result);
extern RC getNumEntries (BTreeHandle *tree, int *result);
extern RC getKeyType (BTreeHandle *tree, DataType *result);

// index access
extern RC findKey (BTreeHandle *tree, Value *key, RID *result);
extern RC insertKey (BTreeHandle *tree, Value *key, RID rid);
extern RC deleteKey (BTreeHandle *tree, Value *key);
extern RC openTreeScan (BTreeHandle *tree, BT_ScanHandle **handle);
extern RC nextEntry (BT_ScanHandle *handle, RID *result);
extern RC closeTreeScan (BT_ScanHandle *handle);

// debug and test functions
extern char *printTree (BTreeHandle *tree);

#endif // BTREE_MGR_H
