#include <string.h>

#include "dt.h"
#include "tables.h"
#include "dberror.h"
#include "rm_page.h"
#include "rm_macros.h"

void
RM_page_deleteAllTuples(RM_Page *self) {
    // Clear storage
    memset(&self->dataBegin, 0, RM_PAGE_DATA_SIZE);

    // Reset any storage flags
    self->header.flags &= (RM_PageFlags) ~RM_PAGE_FLAGS_TUPS_FULL;
    self->header.flags &= (RM_PageFlags) ~RM_PAGE_FLAGS_HAS_TRAILING;

    // Set any other flags to empty
    self->header.flags |= RM_PAGE_FLAGS_HAS_FREE_PTRS;
    self->header.numTuples = 0;
    self->header.freespaceLowerOffset = 0;                      //next available byte in page
    self->header.freespaceUpperEnd = RM_PAGE_DATA_SIZE;         //byte where tuple data starts
    self->header.freespaceTrailingOffset = 0;                   //first available byte after tuple
}

RM_Page *
RM_Page_init(void *buffer, RM_PageNumber pageNumber, RM_PageKind kind) {
    memset(buffer, 0x0, PAGE_SIZE);
    RM_Page *self = buffer;
    self->header.pageNum = pageNumber;
    self->header.kind = kind;
    self->header.nextPageNum = RM_PAGE_NEXT_PAGENUM_UNSET;
    // another page
    RM_page_deleteAllTuples(self);
    return self;
}

RM_PageTuple *
RM_Page_reserveTupleAtEnd(RM_Page *self, uint16_t len) {
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

RM_PageTuple *
RM_reserveTupleAtIndex(
        RM_Page *page,
        const uint16_t len,
        uint16_t slotNum)
{
    const uint16_t initialNumEntries = page->header.numTuples;

    // Reserve a new tuple
    // HACK: We're assuming that a new tuple will always be allocated at the end
    RM_PageTuple *targetTup = RM_Page_reserveTupleAtEnd(page, len);
    uint16_t targetTupOffset = (char *) targetTup - (char *) &page->dataBegin;

    // Determine how we need to fix-up the tuple pointers:
    //  * if we need to insert at index >= 0,
    //    then move all the slot ptrs over by one at and after the insertion index
    //
    //  * if we need to insert at the end,
    //    then just use the end tuple slot we reserved
    if (slotNum != initialNumEntries) {
        // Shift over all the pointers at and after the insertion index by one slot ptr
        RM_PageSlotPtr *targetSlot = ((RM_PageSlotPtr *) &page->dataBegin) + slotNum;
        RM_PageSlotPtr *beginSlot = ((RM_PageSlotPtr *) targetSlot) + 1;
        RM_PageSlotPtr *endSlot = ((RM_PageSlotPtr *) &page->dataBegin) + targetTup->slotId + 1;
        void *dest = (void *) ((RM_PageSlotPtr *) beginSlot + 1);

        if (beginSlot > endSlot) { PANIC("bad pointer locations"); }
        size_t slotsLen = (char *) endSlot - (char *) beginSlot;
        memmove(dest, beginSlot, slotsLen);

        // Re-write the tuple data offset into the target slot
        *targetSlot = targetTupOffset;
        targetTup->slotId = targetTupOffset / sizeof(RM_PageSlotPtr);
    }
    return targetTup;
}

RM_PageTuple *
RM_Page_getTuple(
        RM_Page *self,
        RM_PageSlotId slotIdx,
        RM_PageSlotPtr **ptr_out)
{
    size_t slot = slotIdx * sizeof(RM_PageSlotPtr);
    RM_PageSlotPtr *off = (RM_PageSlotPtr *) (&self->dataBegin + slot);
    RM_PageTuple *tup = (RM_PageTuple *) (&self->dataBegin + *off);

    if (ptr_out != NULL) {
        *ptr_out = off;
    }

    return tup;
}

//assume we always have the right page from calling method 
void
RM_Page_getRecord(RM_Page *self, Record *record, RID rid){
    uint16_t numTuples = self->header.numTuples;
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

void
RM_Page_setTuple(RM_Page *self, Record *r){

    uint16_t numTuples = self->header.numTuples;
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

void
RM_Page_deleteTuple(RM_Page *self, RM_PageSlotId slotId) {
    //locate tuple
    size_t slotOffset = slotId * sizeof(RM_PageSlotPtr);
    RM_PageSlotPtr *off = (RM_PageSlotPtr *) (&self->dataBegin + slotOffset);
    RM_PageTuple *tup = (RM_PageTuple *) (&self->dataBegin + *off);
    memset(tup, 0xef, RM_TUP_SIZE(tup->len));           //zero fill tuple
    memset(off, 0xef, sizeof(RM_PageSlotPtr)); //zero fill slot

    //raise flag in header if deleted one of the middle tups
    //or raise flag if deleted last tup
    if (slotOffset < self->header.freespaceLowerOffset - sizeof(RM_PageSlotId)) {
        self->header.flags |= RM_PAGE_FLAGS_HAS_TRAILING;
    }
    else { /* set freespaceTrailingOffset here*/ }
    //NOT_IMPLEMENTED(); //store free space pointer

    if (slotOffset == self->header.freespaceLowerOffset - sizeof(RM_PageSlotId)) {
        self->header.freespaceLowerOffset -= sizeof(RM_PageSlotId);
    }

    self->header.numTuples--;
}

