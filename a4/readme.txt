ASSIGNMENT 3(Storage Manager)
CS 525 - SPRING 2020
CHRISTOPHER MORCOM & JOSH BOWDEN

! Use the Notes Syntax in Sublime Text 3 to read this with better visuals !

# CORE IMPLEMENTATION NOTES #
Assignment has been additionally been ran under `valgrind` to remove all memory leaks.
Assignment Passes All Tests given.

# EXTRA CREDIT NOTES #
! The B+Tree Manager uses the record manager !
! Pointer Swizzling has been implemented !
! The code is designed to easily extended to the multiple types and variable length keys since we re-use the tuple data structures !
  ! we reuse binfmt.c so we don't have to manually rewrite keys or manually read and write pages !
! Multiple ints of the same value can be added to the key but not deleted. The scan will show them all sequentially though !

Building the Project:
   - run "make" or "make all" to build the Project. 
   - if using *MS PowerShell* use "make psh-clean" to remove the build
   - in linux you can use "make clean"


! Only have one B+Tree Manager Instance Open !

! Index Manager Functions !

  < RC initIndexManager (void *mgmtData) >
    -- creates an instance of the BTree Manager and Record Manager
    -- inits a new page (3) in the page file as a table that stores B+Tree references on disk. 

  < RC shutdownIndexManager () >
    -- frees temporarys struct and shuts down the nested Record Manager

! B+Tree Functions !

  < RC createBtree (char *idxId, DataType keyType, int n) >
    -- inits a BTree tuple in page 3 and a new page that it references. 
      -- it will error check for existing or null entries
    -- if too many trees are created, it will create a new page and reference it from the preceeding page

  < RC openBtree (BTreeHandle **tree, char *idxId) >
    -- mallocates metadata for a tree and generates it
      -- the metadata is made from the header of the root page of the tree.

  < RC closeBtree (BTreeHandle *tree) >
    -- frees the tree pointer and its associated metadata

  < RC deleteBtree (char *idxId) >
    -- deletes the reference to the tree on page 3 using the record manager 


! access information about a b-tree !
  < RC getNumNodes (BTreeHandle *tree, int *result) >
    -- loops through the pages representing the tree
    -- sums the number of nodes in the table header data and stores it in result

  < RC getNumEntries (BTreeHandle *tree, int *result) >
    -- loops through the pages representing the tree
        --  this starts at the leftmost leaf node and will count all nodes from left to right.

  < RC getKeyType (BTreeHandle *tree, DataType *result) >
    -- returns the tree keyType from the tree metadata in the tree pointer


! Key Functions !

  < RC findKey (BTreeHandle *tree, Value *key, RID *result) >
    -- gets the value of the key to be searched
    -- loads the pages containing the tuples to traverse one-by-one 
    -- locates the correct tuple and either errors (300) or stores the result

  < RC insertKey (BTreeHandle *tree, Value *key, RID rid) >
    -- similar to findKey() except it inserts a value and calls helper methods in case of node under/overflow

  < RC deleteKey (BTreeHandle *tree, Value *key) >
    -- similar to findKey() except it inserts a value and calls helper methods in case of node under/overflow


! index access !

  < RC openTreeScan (BTreeHandle *tree, BT_ScanHandle **handle) >
    -- initializes the tree scan metadata to the tree metadata
    -- gets the leftmost leaf node and stores its reference in the metadata

  < RC nextEntry (BT_ScanHandle *handle, RID *result) >
    -- stores the next entry in the node in result and returns or errors out on no more keys left
      -- if at the endo of a node, load the next node's page and store a reference in the metadata 

  < RC closeTreeScan (BT_ScanHandle *handle) >
    -- frees the scan handler's metadata and the handler itself
