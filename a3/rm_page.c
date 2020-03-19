#include <string.h>

#include "dt.h"
#include "dberror.h"
#include "rm_page.h"
#include "rm_macros.h"


RM_Page *RM_Page_init(void *buffer, RM_PageNumber pageNumber, RM_PageKind kind) {
    memset(buffer, 0x0, PAGE_SIZE);
    RM_Page *self = buffer;
    self->header.pageNum = pageNumber;
    self->header.kind = kind;
    self->header.flags = RM_PAGE_FLAGS_HAS_FREE_PTRS;
    self->header.freespaceLowerOffset = 0;
    self->header.freespaceUpperEnd = RM_PAGE_DATA_SIZE;
    self->header.freespaceTrailingOffset = 0;
    self->data = ((char *) buffer) + sizeof(RM_PageHeader);
    return self;
}

void *RM_Page_reserveTuple(RM_Page *self, uint16_t len) {
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
    uint16_t minTupleSpaceRequired = sizeof(RM_PageSlotId) + sizeof(RM_PageSlotLength) + len;
    uint16_t minSpaceRequired = minPtrSpaceRequired + minTupleSpaceRequired;
    uint16_t spaceAvailable = self->header.freespaceUpperEnd - self->header.freespaceLowerOffset;
    if (spaceAvailable < minSpaceRequired) {
        return NULL;
    }

    // Write in next available slot, by determining the starting offset
    uint16_t slotOffset = self->header.freespaceLowerOffset;
    RM_PageSlotPtr *slotPtr = (RM_PageSlotPtr *) (self->data + slotOffset);

    uint16_t tupOffset = self->header.freespaceUpperEnd - minTupleSpaceRequired;
    RM_PageTuple *tup = (RM_PageTuple *) (self->data + tupOffset);

    // Update pointers
    self->header.freespaceLowerOffset += minPtrSpaceRequired;
    self->header.freespaceUpperEnd -= minTupleSpaceRequired;
    if (self->header.freespaceLowerOffset >= self->header.freespaceUpperEnd) {
        self->header.flags &= ~RM_PAGE_FLAGS_HAS_FREE_PTRS;
    }

    uint16_t slotId = slotOffset / sizeof(RM_PageSlotId);
    *slotPtr = tupOffset;
    tup->slotId = slotId;
    tup->len = len;

    void *buf = ((char *) tup) + sizeof(RM_PageSlotId) + sizeof(RM_PageSlotLength);
    tup->data = buf;
    memset(buf, 0xef, len);
    return buf;
}
