#include <string.h>

#include "dt.h"
#include "tables.h"
#include "dberror.h"
#include "rm_page.h"
#include "rm_macros.h"


RM_Page *RM_Page_init(void *buffer, RM_PageNumber pageNumber, RM_PageKind kind) {
    memset(buffer, 0x0, PAGE_SIZE);
    RM_Page *self = buffer;
    self->header.pageNum = pageNumber;
    self->header.kind = kind;
    self->header.flags = RM_PAGE_FLAGS_HAS_FREE_PTRS;
    self->header.numTuples = 0;
    self->header.freespaceLowerOffset = 0;                      //next available byte in page
    self->header.freespaceUpperEnd = RM_PAGE_DATA_SIZE;         //byte where tuple data starts
    self->header.freespaceTrailingOffset = 0;                   //first available byte after tuple
    self->header.nextPageNum = -1;                              //-1 if no tuples on another page
    return self;
}

RM_PageTuple *RM_Page_reserveTuple(RM_Page *self, uint16_t len) { 
    if (len > RM_PAGE_DATA_SIZE) {
        PANIC("`len` was greater than `RM_PAGE_DATA_SIZE`");
    }

    // Check if the page is full in the first place
    RM_PageFlags flags = self->header.flags;
    if (IS_FLAG_UNSET(flags, RM_PAGE_FLAGS_HAS_FREE_PTRS)
        || IS_FLAG_SET(flags, RM_PAGE_FLAGS_TUPS_FULL)) {
        return NULL;
    }

    // Check if we have enough empty space to write the tuple
    uint16_t minPtrSpaceRequired = sizeof(RM_PageSlotPtr);
    uint16_t minTupleSpaceRequired = RM_TUP_SIZE(len);
    uint16_t minSpaceRequired = minPtrSpaceRequired + minTupleSpaceRequired;
    uint16_t spaceAvailable = self->header.freespaceUpperEnd - self->header.freespaceLowerOffset;
    
    //check if there is no space in page
    if (spaceAvailable < minSpaceRequired) {
        return NULL;
    }

    // Given that we know that we have space, increment the number of tuples
    self->header.numTuples++;

    // Write in next available slot, by determining the starting offset
    uint16_t slotOffset = self->header.freespaceLowerOffset;
    RM_PageSlotPtr *slotPtr = (RM_PageSlotPtr *) (&self->dataBegin + slotOffset);

    uint16_t tupOffset = self->header.freespaceUpperEnd - minTupleSpaceRequired;
    RM_PageTuple *tup = (RM_PageTuple *) (&self->dataBegin + tupOffset);

    // Update pointers
    self->header.freespaceLowerOffset += minPtrSpaceRequired;
    self->header.freespaceUpperEnd -= minTupleSpaceRequired;
    if (self->header.freespaceLowerOffset >= self->header.freespaceUpperEnd) {
        self->header.flags &= ~RM_PAGE_FLAGS_HAS_FREE_PTRS;
    }

    uint16_t slotId = slotOffset / sizeof(RM_PageSlotId);
    //printf("rm_page: [pg#%d] self->data = %p; tupOffset = 0x%x\n", self->header.pageNum, &self->dataBegin, tupOffset);
    fflush(stdout);
    *slotPtr = tupOffset;
    tup->slotId = slotId;
    tup->len = len;
    return tup;
}

//assume we always have the right page from calling method 
void *RM_Page_getTuple(RM_Page *self, Record *record, RID rid){
    uint16_t numTuples = self->header.numTuples;
    printf("numtups: %d\n", numTuples);
    int slotNum = rid.slot; 
    if (slotNum >= numTuples) PANIC("slotNum > max slots in page");

    //get position of tuple
    size_t slot = slotNum * sizeof(RM_PageSlotPtr);
    RM_PageSlotPtr *off = (RM_PageSlotPtr *) (&self->dataBegin + slot);

    RM_PageTuple *tup = (RM_PageTuple *) (&self->dataBegin + *off);

    //read the data and assign it into the record ptr
    RM_PageSlotLength n = tup->len;
    void *buf = malloc(n);
    memcpy(buf, &tup->dataBegin, n);

    record->id = rid;
    record->data = buf;
}

void *RM_Page_setTuple(RM_Page *self, Record *r){

    uint16_t numTuples = self->header.numTuples;
    printf("numtups: %d\n", numTuples);
    int slotNum = r->id.slot; 
    if (slotNum >= numTuples) PANIC("slotNum > max slots in page");

    //get position of tuple
    size_t slot = slotNum * sizeof(RM_PageSlotPtr);
    RM_PageSlotPtr *off = (RM_PageSlotPtr *) (&self->dataBegin + slot);

    RM_PageTuple *tup = (RM_PageTuple *) (&self->dataBegin + *off);
    RM_PageSlotLength n = tup->len;

    //copy record into tuple
    memcpy(&tup->dataBegin, (void *)r->data, n);
}
