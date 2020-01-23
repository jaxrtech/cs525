#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "storage_mgr.h"
#include "dberror.h"
#include "test_helper.h"

// test name
char *testName;

/* test output files */
#define TESTPF "test_pagefile.bin"

/* prototypes for test functions */
static void testCreateOpenClose(void);
static void testSinglePageContent(void);
static void testAll(void);

/* main function running all tests */
int
main (void)
{
  testName = "";
  
  initStorageManager();

  testCreateOpenClose();
  testSinglePageContent();

  testAll();  //EXTRA TESTING

  return 0;
}


/* check a return code. If it is not RC_OK then output a message, error description, and exit */
/* Try to create, open, and close a page file */
void
testCreateOpenClose(void)
{
  SM_FileHandle fh;

  testName = "test create open and close methods";

  // Ensure that we're starting from scratch
  destroyPageFile (TESTPF);
  TEST_CHECK(createPageFile (TESTPF));
  
  TEST_CHECK(openPageFile (TESTPF, &fh));
  ASSERT_TRUE(strcmp(fh.fileName, TESTPF) == 0, "filename correct");
  ASSERT_TRUE((fh.totalNumPages == 1), "expect 1 page in new file");
  ASSERT_TRUE((fh.curPagePos == 0), "freshly opened file's page position should be 0");

  TEST_CHECK(closePageFile (&fh));
  TEST_CHECK(destroyPageFile (TESTPF));

  // after destruction trying to open the file should cause an error
  ASSERT_TRUE((openPageFile(TESTPF, &fh) != RC_OK), "opening non-existing file should return an error.");

  TEST_DONE();
}

/* Try to create, open, and close a page file */
void
testSinglePageContent(void)
{
  SM_FileHandle fh;
  SM_PageHandle ph;
  int i;

  testName = "test single page content";

  ph = (SM_PageHandle) malloc(PAGE_SIZE);

  // create a new page file
  TEST_CHECK(createPageFile (TESTPF));
  TEST_CHECK(openPageFile (TESTPF, &fh));
  printf("created and opened file\n");
  ASSERT_EQUALS_INT(
          1, getTotalNumBlocks(&fh),
          "expected newly created page file to have exactly 1 block");

  // read first page into handle
  TEST_CHECK(readFirstBlock (&fh, ph));

  // the page should be empty (zero bytes)
  for (i=0; i < PAGE_SIZE; i++)
    ASSERT_TRUE((ph[i] == 0), "expected zero byte in first page of freshly initialized page");
  printf("first block was empty\n");
  // change ph to be a string and write that one to disk
  for (i=0; i < PAGE_SIZE; i++)
    ph[i] = (i % 10) + '0';
  TEST_CHECK(writeBlock (0, &fh, ph)); 
  printf("writing first block\n");

  for (i=0; i < PAGE_SIZE; i++){ ph[i] = 'a'; } //reset char array to invalid values 

  // read back the page containing the string and check that it is correct
  TEST_CHECK(readFirstBlock (&fh, ph));
  for (i=0; i < PAGE_SIZE; i++)
    ASSERT_TRUE((ph[i] == (i % 10) + '0'), "character in page read from disk is the one we expected.");
  printf("reading first block\n");

  free(ph);
  // destroy new page file
  TEST_CHECK(destroyPageFile(TESTPF));  
  
  TEST_DONE();
}


void testAll()
{
  SM_FileHandle fh = {};
  SM_PageHandle ph;

  int i;
  testName = "test multi-page content";
  ph = (SM_PageHandle) malloc(PAGE_SIZE);
  memset(ph, '.', PAGE_SIZE);

  // expect error if reading or writing unopened pagefile
  ASSERT_TRUE((writeBlock(0, &fh, ph) != RC_OK), "cannot write to unopened pagefile");
  ASSERT_TRUE((readBlock(0, &fh, ph) != RC_OK), "cannot read unopened pagefile");

  //write three blocks and test if the values are correct
  destroyPageFile(TESTPF);
  TEST_CHECK(createPageFile(TESTPF));
  TEST_CHECK(openPageFile(TESTPF, &fh));
  ASSERT_TRUE((getBlockPos(&fh) == 0), "new pagefile has zero blocks");        //ensure starting at zero pages
  printf("created and opened pagefile\n");

  TEST_CHECK(writeBlock(0, &fh, ph)); 
  TEST_CHECK(readFirstBlock(&fh, ph));
  for (i=0; i < PAGE_SIZE; i++) {
      ASSERT_TRUE((ph[i] == '.'),"character in page read from disk is the one we expected.");
  }
  ASSERT_TRUE((getBlockPos(&fh) == 0), "current block position correct");        //ensure written 1 page
  printf("wrote and read 1st block\n");

  TEST_CHECK(appendEmptyBlock(&fh));
  TEST_CHECK(readBlock(1, &fh, ph));
  ASSERT_TRUE((getBlockPos(&fh) == 1), "current block position correct");        //ensure written 1+NULL pages
  for (i = 0; i < PAGE_SIZE; i++)
    ASSERT_TRUE((ph[i] == '\0'), "character in page read from disk is the one we expected.");
  printf("wrote a NULL block\n");

  memset(ph, 'k', PAGE_SIZE); 
  TEST_CHECK(writeCurrentBlock(&fh, ph));  //rewrite null block to 'kkkkkk...'
  TEST_CHECK(readLastBlock(&fh, ph));
  for (i=0; i < PAGE_SIZE; i++)
    ASSERT_TRUE((ph[i] == 'k'), "character in page read from disk is the one we expected.");
  ASSERT_TRUE((getBlockPos(&fh) == 1), "current block position correct");        //ensure written 2 pages
  printf("over-wrote NULL (2nd) block correctly\n");

  memset(ph, '-', PAGE_SIZE); 
  TEST_CHECK(writeNewBlock(&fh, ph));
  TEST_CHECK(readLastBlock(&fh, ph));
  for (i=0; i < PAGE_SIZE; i++)
    ASSERT_TRUE((ph[i] == '-'), "character in page read from disk is the one we expected.");
  ASSERT_TRUE((getBlockPos(&fh) == 2), "current block position correct");        //ensure written 3 pages
  printf("wrote 3rd block\n");
  TEST_CHECK(ensureCapacity(5, &fh));
  ASSERT_TRUE((getBlockPos(&fh) == 2), "current block position correct");        //ensure written 3 pages
  ASSERT_TRUE((getTotalNumBlocks(&fh) == 5), "total blocks in pagefile correct");//ensure 5 writable pages
  printf("ensured pagefile has 5 blocks\n");
  
  closePageFile(&fh);
  destroyPageFile(TESTPF);  

  free(ph);
  TEST_DONE();
}