ASSIGNMENT 3(Storage Manager)
CS 525 - SPRING 2020
CHRISTOPHER MORCOM & JOSH BOWDEN

! Use the Notes Syntax in Sublime Text 3 to read this with better visuals !

Building the Project:
   - run "make" to build the Project. 
   - if using *MS PowerShell* use "make psh-clean" to remove the build
   - in linux you can use "make clean"

Assignment has been additionally been ran under `valgrind` to remove all memory leaks.

! Only have one Record Manager Instance Open !

Table and Record Manager Functions:

  < RC initRecordManager(void *) >
      -- initializes a buffer manager and writes the database header page and schema pages
      -- RM can function as a DB of DBs if we want it to
      -- RM will create a pagefile to store the data

  < RC shutdownRecordManager(void) >
      -- shuts down the buffer manager and frees memory associated with RM

  < RC createTable(char *name, Schema *schema) >
      -- checks for a pre-existing table and raises an error for duplicates
      -- copies *schema* information to disk
      -- initializes a new tuple in the schema page and creates a page for the table

  < RC openTable(RM_TableData *rel, char *name) >
      -- loads a table named *name* into empty table *rel*
      -- pins the first data page and loads its header information

  < RC closeTable(RM_TableData *rel) >
      -- flushes the buffer pool
      -- writes dirty pages to disk and frees *rel* memory  

  < RC deleteTable(char *name) >
      -- assumes that the table is closed and loads it into memory to read header data
      -- checks if there are overflow pages and deletes pages in a linked-list fashion by looping pages

  < int getNumTuples(RM_TableData *rel) >
      -- will load the first page of the table *rel* 
      -- increments total tuple counter bu number of tuples in page
      -- scans any overflow pages and increments the counter
      -- returns the counter when there are no more pages for *rel* left to scan

Schema Functions:

  < Schema *createSchema(int numAttr, char **attrNames, DataType *dataTypes, int *typeLength, int keySize, int *keys) >
      -- given schema attributes, this will create a schema and return a pointer to it 

  < RC freeSchema(Schema *schema) >
         -- frees *schema*

  < RC getAttr(Record *record, Schema *schema, int attrNum, Value **value) >
      -- loads the schema and its attributes
      -- finds a pointer to attribute specified by *attrNum* by using offsets on record data
      -- bias the union data using the attribute DataType and stores it in **value**

  < RC setAttr(Record *record, Schema *schema, int attrNum, Value *value) >
      -- does the same thing as *getAttr* but moves data from **value** to record->data

Record Functions:

  < RC getRecord(RM_TableData *rel, RID id, Record *record) >
      -- open table *rel* and pin the record ID page *RID page*
         -- if it exists we check the slot from *RID slot* in the page 
         -- set the record pointer from the user to the record (copied from memory) or error out

  < RC createRecord(Record **record, Schema *schema) >
      -- gets the record size from the *schema* based on the schema dataTypes
      -- allocates a record that will be returned later
      -- initializes the record and its data pointer
      -- points to the record using a user-defined pointer

  < RC insertRecord(RM_TableData *rel, Record *record) >
      -- loop through the *rel* table's pages read the page that we will be writing to and pin it
         -- on each iteration we look for a free page slot by checking the page header 
         -- write a new page if we have to and link it to the table
         -- populate a slot in the page and point it to a record using offsets. 
         -- write the tuple at the slot pointer

  < RC updateRecord(RM_TableData *rel, Record *record) >
      -- read the page that we will be writing to and pin it
      -- loop through *rel* like in insertRecord() and linearly search for the record slot pointer to update
      -- write a new tuple over the old one located at the slot pointer

  < RC deleteRecord(RM_TableData *rel, RID id) >
      -- read the page that we will be writing to and pin it
      -- loop through *rel* like in insertRecord() and linearly search for the record slot pointer to delete
      -- empty the slot pointer and raise flag in header that the slot is empty

  < RC freeRecord(Record *record) >
      -- frees the record pointer

  < int getRecordSize(Schema *schema) >
      -- looks at the schma attributes to determine size and returns it

Scan Functions:

  < RC startScan(RM_TableData *rel, RM_ScanHandle *scan, Expr *cond) >
      -- populates the *ScanHandle* with a pointer to the table data *rel*, the buffer pool, and the scan condition
      -- also allocates an *RID* to store the last searched index and initializes it to the table page and first slot

  < RC next(RM_ScanHandle *scan, Record *record) >
      -- unpacks the data from the *ScanHandle*
      -- pins the current working page gathered from the *RID* in the *ScanHandle*
      -- linearly check each record if it satisfies the condition and store it in a record pointer from the user
      -- increment the *RID* to the next slot (or 0'th slot if increment the page to check)
      -- returns RC_OK if we found a tuple (hit)
      -- next() will loop until an error saying we scanned everything occurs or we get a hit
      -- if there isn't anything left to scan, it will return an error: RC_RM_NO_MORE_TUPLES (203)

  < RC closeScan(RM_ScanHandle *scan) >
      -- frees the temporary RID we made for tracking and returns

