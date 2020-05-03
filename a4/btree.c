#include "btree_mgr.h"
#include "binfmt.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "record_mgr.h"
#include "buffer_mgr.h"
#include "tables.h"
#include "btree.h"
#include "rm_macros.h"
#include "rm_page.h"
#include "btree_binfmt.h"

void IM_IndexMetadata_makeFromMessage(
        IM_IndexMetadata *meta,
        IM_DESCRIPTOR_FORMAT_T *indexMsg)
{
    PANIC_IF_NULL(meta);
    PANIC_IF_NULL(indexMsg);

    meta->rootNodePageNum = BF_AS_U16(indexMsg->idxRootNodePageNum);
    meta->maxEntriesPerNode = BF_AS_U16(indexMsg->idxMaxEntriesPerNode);
}

RID IM_makeRidFromEntry(
        IM_ENTRY_FORMAT_T *entry)
{
    PANIC_IF_NULL(entry);

    RID rid;
    rid.page = BF_AS_U16(entry->idxEntryRidPageNum);
    rid.slot = BF_AS_U16(entry->idxEntryRidSlot);

    return rid;
}

void IM_NodeTrace_append(IM_NodeTrace *self, RID val)
{
    if (self->entryLen == self->entryCapacity) {
        self->entryCapacity = self->entryCapacity << 1u; // next power of 2
        self->entryArr = realloc(self->entryArr, self->entryCapacity);
    }

    self->entryArr[self->entryLen] = val;
    self->entryLen++;
}

void IM_NodeTrace_free(IM_NodeTrace *self)
{
    free(self->entryArr);
    free(self);
}

// called by record manager -- do not mark as `static`
RC IM_writeIndexPage(BM_BufferPool *pool)
{
    BM_PageHandle pageHandle = {};
    TRY_OR_RETURN(pinPage(pool, &pageHandle, RM_PAGE_INDEX));

    RM_Page_init(pageHandle.buffer, RM_PAGE_INDEX, RM_PAGE_KIND_INDEX);

    TRY_OR_RETURN(forcePage(pool, &pageHandle));
    TRY_OR_RETURN(unpinPage(pool, &pageHandle));

    return RC_OK;
}

void IM_readEntry_i32(RM_PageTuple *tup, IM_ENTRY_FORMAT_T *result)
{
    PANIC_IF_NULL(tup);
    PANIC_IF_NULL(result);
    if (tup->len == 0) { PANIC("length of 'tup' buffer cannot be zero"); }

    memcpy(result, &IM_ENTRY_FORMAT_OF_I32, sizeof(IM_ENTRY_FORMAT_OF_I32));
    uint16_t read = BF_read(
            (BF_MessageElement *) result,
            &tup->dataBegin,
            BF_NUM_ELEMENTS(sizeof(*result)));

    if (read == 0) { PANIC("read failed"); }
    if (read > tup->len) {
        PANIC("buffer overrun: got %d bytes, but buffer is %d bytes",
                read,
                tup->len);
    }
}

void IM_writeEntry_i32(RM_PageTuple *tup, IM_ENTRY_FORMAT_T *entry)
{
    PANIC_IF_NULL(tup);
    PANIC_IF_NULL(entry);
    if (tup->len == 0) { PANIC("length of 'tup' buffer cannot be zero"); }

    uint16_t wrote = BF_write(
            (BF_MessageElement *) entry,
            &tup->dataBegin,
            BF_NUM_ELEMENTS(sizeof(*entry)));

    if (wrote == 0) {
        PANIC("failed to write entry"); }
    if (wrote > tup->len) {
        PANIC("buffer overrun: attempted to write %d bytes, but buffer was %d bytes",
              wrote,
              tup->len);
    }
}

RC IM_readEntryAt_i32(BM_BufferPool *pool, RID rid, IM_ENTRY_FORMAT_T *entry_out)
{
    PANIC_IF_NULL(entry_out);

    BM_PageHandle pageHandle = {};
    TRY_OR_RETURN(pinPage(pool, &pageHandle, rid.page));
    RM_Page *page = (RM_Page *) pageHandle.buffer;

    RM_PageTuple *tup = RM_Page_getTuple(page, rid.slot, NULL);
    IM_readEntry_i32(tup, entry_out);

    LOG_DEBUG("[%d.%d] -> | key = %d, ptr = [%d.%d]",
            rid.page,
            rid.slot,
            BF_AS_I32(entry_out->idxEntryKey),
            BF_AS_U16(entry_out->idxEntryRidPageNum),
            BF_AS_U16(entry_out->idxEntryRidSlot));

    TRY_OR_RETURN(unpinPage(pool, &pageHandle));
    return RC_OK;
}

RC IM_readEntryValueAt(BM_BufferPool *pool, RID entryIndex, RID *entryValue_out)
{
    PANIC_IF_NULL(entryValue_out);

    BM_PageHandle pageHandle = {};
    TRY_OR_RETURN(pinPage(pool, &pageHandle, entryIndex.page));
    RM_Page *page = (RM_Page *) pageHandle.buffer;

    RM_PageTuple *tup = RM_Page_getTuple(page, entryIndex.slot, NULL);
    IM_ENTRY_FORMAT_T entry;
    IM_readEntry_i32(tup, &entry);

    RID result = {
            .page = BF_AS_U16(entry.idxEntryRidPageNum),
            .slot = BF_AS_U16(entry.idxEntryRidSlot)};

    *entryValue_out = result;

    TRY_OR_RETURN(unpinPage(pool, &pageHandle));
    return RC_OK;
}

RC IM_writeEntryAt_i32(
        BM_BufferPool *pool,
        RID rid,
        IM_ENTRY_FORMAT_T *entry_out)
{
    PANIC_IF_NULL(entry_out);
    LOG_DEBUG("[%d.%d] <- | key = %d, ptr = [%d.%d] |",
            rid.page,
            rid.slot,
            BF_AS_I32(entry_out->idxEntryKey),
            BF_AS_U16(entry_out->idxEntryRidPageNum),
            BF_AS_U16(entry_out->idxEntryRidSlot));

    BM_PageHandle pageHandle = {};
    TRY_OR_RETURN(pinPage(pool, &pageHandle, rid.page));
    RM_Page *page = (RM_Page *) pageHandle.buffer;

    RM_PageTuple *tup = RM_Page_getTuple(page, rid.slot, NULL);
    IM_writeEntry_i32(tup, entry_out);

    TRY_OR_RETURN(markDirty(pool, &pageHandle));
    TRY_OR_RETURN(unpinPage(pool, &pageHandle));
    return RC_OK;
}

void IM_makeEntry_i32(
        IM_ENTRY_FORMAT_T *entry,
        int32_t key,
        RID rid)
{
    PANIC_IF_NULL(entry);
    if ((rid.page < 0) || (rid.page > UINT16_MAX)) { PANIC("'rid.page' out of bounds"); }
    if ((rid.slot < 0) || (rid.slot > UINT16_MAX)) { PANIC("'rid.slot' out of bounds"); }

    *entry = IM_ENTRY_FORMAT_OF_I32;
    BF_SET_I32(entry->idxEntryKey) = key;
    BF_SET_U16(entry->idxEntryRidPageNum) = rid.page;
    BF_SET_U16(entry->idxEntryRidSlot) = rid.slot;
}

RC IM_findIndex(
        BM_BufferPool *pool,
        char *name,
        BM_PageHandle *descriptor_page_out_opt,
        struct RM_PageTuple **tup_out_opt,
        struct IM_DESCRIPTOR_FORMAT_T *msg_out_opt)
{
    PANIC_IF_NULL(pool);
    PANIC_IF_NULL(name);
    if (tup_out_opt != NULL && descriptor_page_out_opt == NULL) {
        PANIC("cannot output tuple ptr if page handle ptr not provided");
    }

    size_t nameLength = strlen(name);

    //store the schema page in pageHandle
    BM_PageHandle pageHandle = {};
    TRY_OR_RETURN(pinPage(pool, &pageHandle, RM_PAGE_INDEX));

    //open the schema page and store the header and data
    RM_Page *pg = (RM_Page*) pageHandle.buffer;
    RM_PageHeader *hdr = &pg->header;
    //as long as this isn't zero, it is an offset from the header to the tuple
    //increment it by 2 bytes to get the next pointer
    RM_PageSlotPtr *off; //deref this address for the offset

    //find the pointer in the data that points to the proper table tuple (or error if table doesn't exist)
    RM_PageTuple *tup = NULL;
    uint16_t num = hdr->numTuples;
    bool hit = false;
    struct IM_DESCRIPTOR_FORMAT_T indexMsg = {};
    for (int i = 0; i < num; i++) {
        size_t slot = i * sizeof(RM_PageSlotPtr);
        off = (RM_PageSlotPtr *) (&pg->dataBegin + slot);
        if (*off >= RM_PAGE_DATA_SIZE) {
            PANIC("tuple offset cannot be greater than page data size");
        }

        //printf("openTable: [tup#%d] pg = %p, pg->data = %p, slot_off = %d, off = %d\n", i, pg, &pg->dataBegin, slot, *off);
        //fflush(stdout);
        tup = (RM_PageTuple *) (&pg->dataBegin + *off);

        indexMsg = IM_DESCRIPTOR_FORMAT;
        BF_read((BF_MessageElement *) &indexMsg, &tup->dataBegin, BF_NUM_ELEMENTS(sizeof(IM_DESCRIPTOR_FORMAT)));
        int idxNameLength = BF_STRLEN(indexMsg.idxName) - 1; // BF will store the extra '\0'
        if (idxNameLength <= 0) {
            PANIC("bad index name length = %d", idxNameLength);
        }

        if (nameLength == idxNameLength
            && memcmp(BF_AS_STR(indexMsg.idxName), name, idxNameLength) == 0)
        {
            hit = true;
            break;
        }
    }

    if (!hit) {
        return RC_IM_KEY_NOT_FOUND;
    }

    if (tup_out_opt != NULL) {
        *tup_out_opt = tup;
    }

    if (descriptor_page_out_opt != NULL) {
        *descriptor_page_out_opt = pageHandle;
    }
    else {
        TRY_OR_RETURN(unpinPage(pool, &pageHandle));
    }

    if (msg_out_opt != NULL) {
        memcpy(msg_out_opt, &indexMsg, sizeof(IM_DESCRIPTOR_FORMAT));
    }

    return RC_OK;
}


RC IM_insertSplit_byAllocLeftRight(
        RM_Page *oldPage,
        int32_t keyValue,
        IM_ENTRY_FORMAT_T *entry,
        uint16_t entryDescriptorSize,
        BM_BufferPool *pool,
        uint16_t maxEntriesPerNode,
        IM_SplitLeafNodeCtx *ctx_out)
{
    if (entryDescriptorSize % sizeof(BF_MessageElement) != 0) {
        PANIC("expected 'entrySize' to be a multiple of 'sizeof(BF_MessageElement)`. "
              "make sure that you using the in-memory 'sizeof(entry)' NOT 'BF_recomputeSize()'");
    }

    BP_Metadata *meta = pool->mgmtData;
    RM_PageHeader *pageHeader = &oldPage->header;
    const uint16_t initialNumEntries = pageHeader->numTuples;

    // Recalculate the physical size of the entry
    RM_PageSlotLength entryPhysSize = BF_recomputePhysicalSize(
            (BF_MessageElement *) entry,
            BF_NUM_ELEMENTS(entryDescriptorSize));

    // Determine the index at which the entry would normally be inserted
    uint16_t targetSlotIdx = IM_getEntryInsertionIndex(keyValue, oldPage, maxEntriesPerNode);

    // Allocate the left anf right leaf nodes
    int leftPageNum = meta->fileHandle->totalNumPages;
    int rightPageNum = leftPageNum + 1;

    BM_PageHandle leftPageHandle = {};
    BM_PageHandle rightPageHandle = {};
    TRY_OR_RETURN(pinPage(pool, &leftPageHandle, leftPageNum));
    TRY_OR_RETURN(pinPage(pool, &rightPageHandle, rightPageNum));

    // Initialize pages
    RM_Page *leftPage = RM_Page_init(leftPageHandle.buffer, leftPageNum, RM_PAGE_KIND_INDEX);
    RM_Page *rightPage = RM_Page_init(rightPageHandle.buffer, rightPageNum, RM_PAGE_KIND_INDEX);

    // Unset leaf flag from the old now inner node
    UNSET_FLAG(oldPage->header.flags, RM_PAGE_FLAGS_INDEX_LEAF);
    SET_FLAG(oldPage->header.flags, RM_PAGE_FLAGS_INDEX_INNER);

    // Set indexing flags
    SET_FLAG(leftPage->header.flags, RM_PAGE_FLAGS_INDEX_LEAF);
    SET_FLAG(rightPage->header.flags, RM_PAGE_FLAGS_INDEX_LEAF);

    // Link the left node to the right node, and the right node to the original next
    leftPage->header.nextPageNum = rightPageNum;
    rightPage->header.nextPageNum = oldPage->header.nextPageNum;

    // Check if the max number of entries is even.
    // If so, the left node gets the extra entry.
    uint16_t numLeftFill;
    bool isMaxEven = maxEntriesPerNode % 2 == 0;
    if (isMaxEven) {
        // ex: if the max == 2, then 2 nodes left, 1 node right
        numLeftFill = maxEntriesPerNode;
    } else {
        // ex: if the max == 5, then 3 nodes left, 3 nodes right
        numLeftFill = (maxEntriesPerNode + 1) / 2;
    }

    // Fill the left until it's "full"
    int16_t oldPageShift = 0; // use when we pass the insertion point, to offset items
    uint16_t i;
    for (i = 0; i < numLeftFill; i++) {
        if (i != targetSlotIdx) {
            // Get the old node
            RM_PageTuple *oldTup = RM_Page_getTuple(oldPage, i + oldPageShift, NULL);
            RM_PageSlotLength len = oldTup->len;

            // Reserve the tuple in the new node to copy the old one
            RM_PageTuple *newTup = RM_Page_reserveTupleAtEnd(leftPage, len);

            // Copy the entry over
            memcpy(&newTup->dataBegin, &oldTup->dataBegin, len);
        }
        else {
            // We are not writing a tuple from the old node
            // Decrement the page shift
            oldPageShift--;

            // Write out new entry
            RM_PageTuple *newTup = RM_Page_reserveTupleAtEnd(leftPage, entryPhysSize);
            IM_writeEntry_i32(newTup, entry);
        }
    }

    // Fill the rest of the tuples into the right node
    uint16_t maxSlotEntries = initialNumEntries + 1; // add new key
    for (i = numLeftFill; i < maxSlotEntries; i++) {
        if (i != targetSlotIdx) {
            // Get the old node
            RM_PageTuple *oldTup = RM_Page_getTuple(oldPage, i + oldPageShift, NULL);
            RM_PageSlotLength len = oldTup->len;

            // Reserve the tuple in the new node to copy the old one
            RM_PageTuple *newTup = RM_Page_reserveTupleAtEnd(rightPage, len);

            // Copy the entry over
            memcpy(&newTup->dataBegin, &oldTup->dataBegin, len);
        }
        else {
            // This is where the node we're inserting will go
            // Increment the page shift by one
            oldPageShift--;

            // Write out new entry
            RM_PageTuple *newTup = RM_Page_reserveTupleAtEnd(rightPage, entryPhysSize);
            IM_writeEntry_i32(newTup, entry);
        }
    }

    // Clear all nodes on the old page and reset flags
    RM_Page_deleteAllTuples(oldPage);

    TRY_OR_RETURN(forcePage(pool, &leftPageHandle));
    TRY_OR_RETURN(forcePage(pool, &rightPageHandle));

    TRY_OR_RETURN(unpinPage(pool, &leftPageHandle));
    TRY_OR_RETURN(unpinPage(pool, &rightPageHandle));

    bool isTargetLeft = targetSlotIdx < numLeftFill;
    if (ctx_out != NULL) {
        ctx_out->leftPageNum = leftPageNum;
        ctx_out->rightPageNum = rightPageNum;
        ctx_out->isTargetLeft = isTargetLeft;
        ctx_out->targetSlot =
                isTargetLeft
                ? targetSlotIdx
                : targetSlotIdx - numLeftFill;
    }

    return RC_OK;
}

RC IM_insertSplit_byReuseLeftAllocRight(
        RM_Page *oldPage,
        int32_t keyValue,
        IM_ENTRY_FORMAT_T *entry,
        uint16_t entryDescriptorSize,
        BM_BufferPool *pool,
        uint16_t maxEntriesPerNode,
        IM_SplitLeafNodeCtx *ctx_out)
{
    if (maxEntriesPerNode == 0) { NOT_IMPLEMENTED(); }

    if (entryDescriptorSize % sizeof(BF_MessageElement) != 0) {
        PANIC("expected 'entrySize' to be a multiple of 'sizeof(BF_MessageElement)`. "
              "make sure that you using the in-memory 'sizeof(entry)' NOT 'BF_recomputeSize()'");
    }

    BP_Metadata *meta = pool->mgmtData;
    RM_PageHeader *pageHeader = &oldPage->header;
    const uint16_t initialNumEntries = pageHeader->numTuples;

    const RM_PageSlotLength entryPhysSize = BF_recomputePhysicalSize(
            (BF_MessageElement *) entry,
            BF_NUM_ELEMENTS(entryDescriptorSize));

    if (entryPhysSize == 0) {
        PANIC("expected 'entryPhysSize' to be > 0");
    }

    // Determine the index at which the entry would normally be inserted
    uint16_t targetSlotIdx = IM_getEntryInsertionIndex(keyValue, oldPage, maxEntriesPerNode);

    // Allocate a new node for the *right* leaf
    // We will reuse the old page as the *left* leaf node since then we can
    // avoid messing with a previous sibling leaf node's next pointer.
    int leftPageNum = oldPage->header.pageNum;
    int rightPageNum = meta->fileHandle->totalNumPages;

    BM_PageHandle rightPageHandle = {};
    TRY_OR_RETURN(pinPage(pool, &rightPageHandle, rightPageNum));

    // Set the *left* leaf page to the old page
    // Initialize new *right* leaf page
    RM_Page *leftPage = oldPage;
    RM_Page *rightPage = RM_Page_init(rightPageHandle.buffer, rightPageNum, RM_PAGE_KIND_INDEX);

    // Set indexing flags
    SET_FLAG(leftPage->header.flags, RM_PAGE_FLAGS_INDEX_LEAF);
    SET_FLAG(rightPage->header.flags, RM_PAGE_FLAGS_INDEX_LEAF);

    // Link the left node to the right node, and the right node to the original next
    RM_PageNumber oldPageNext = oldPage->header.nextPageNum;
    leftPage->header.nextPageNum = rightPageNum;
    rightPage->header.nextPageNum = oldPageNext;

    // Check if the max number of entries is even.
    // If so, the left node gets the extra entry.
    uint16_t numRightFill;
    bool isMaxEven = maxEntriesPerNode % 2 == 0;
    if (isMaxEven) {
        // ex: if the max == 4, but N = 5, then 3 nodes left, 2 node right
        numRightFill = maxEntriesPerNode / 2;
    } else {
        // ex: if the max == 5, but N = 6, then 3 nodes left, 3 nodes right
        numRightFill = (maxEntriesPerNode + 1) / 2;
    }

    uint16_t numLeftFill = (oldPage->header.numTuples + 1) - numRightFill;
    uint16_t firstRightAbsSlotId = numLeftFill;
    uint16_t lastAbsSlotId = oldPage->header.numTuples;
    bool isTargetLeft = targetSlotIdx < numLeftFill;

    //
    // We're going to work backwards so that we don't override data.
    // Hence we first will need to move over anything that belongs in the
    // *new* right node.
    //

    //
    // Write out the right node
    //

    uint16_t i;
    int oldPageOff = isTargetLeft ? -1 : 0;
    for (i = firstRightAbsSlotId; i <= lastAbsSlotId; i++) {
        if (isTargetLeft && i == targetSlotIdx) { PANIC("invalid state"); }

        RM_PageTuple *oldTup = NULL;
        uint16_t len;
        if (i == targetSlotIdx) {
            // If we hit the position at which we would insert the target,
            // use the entry length
            len = entryPhysSize;
        }
        else {
            // Otherwise, get the old tuple length
            oldTup = RM_Page_getTuple(oldPage, i + oldPageOff, NULL);
            len = oldTup->len;
        }

        RM_PageTuple *newTup = RM_Page_reserveTupleAtEnd(rightPage, len);

        if (i == targetSlotIdx) {
            // Copy the insertion entry into the current tuple
            IM_writeEntry_i32(newTup, entry);

            // Decrement the page shift by one since we didn't read from the
            // old page
            oldPageOff--;
        }
        else {
            // Copy the old tuple to the new *right* tuple
            //
            // NOTE: Since we're writing into the new *right* node
            // left-to-right, we can be sure that we are not overwriting any
            // tuples
            memcpy(&newTup->dataBegin, &oldTup->dataBegin, len);
        }
    }

    //
    // Adjust the old *left* node
    //

    oldPageOff = 0; // reset the offset
    if (targetSlotIdx == numLeftFill - 1) {
        // We're inserting the target node in the last position in the left node
        // In this case, we just need to overwrite the last entry
        RM_PageTuple *tup = RM_Page_getTuple(oldPage, targetSlotIdx, NULL);
        IM_writeEntry_i32(tup, entry);
    }
    else if (targetSlotIdx < numLeftFill - 1) {
        // We're inserting the target node before the last element in the
        // new *left* node. We're going to have to shift the slot pointers in
        // the left node over by one to the right.

        // Delete the last element first to prevent an overflow at insert time
        RM_Page_deleteTuple(oldPage, numLeftFill - 1);

        // Insert the tuple, by shifting the rest of the entries over after it
        RM_PageTuple *tup = RM_Page_reserveTupleAtIndex(oldPage, targetSlotIdx, entryPhysSize);
        IM_writeEntry_i32(tup, entry);
    }

    TRY_OR_RETURN(forcePage(pool, &rightPageHandle));
    TRY_OR_RETURN(unpinPage(pool, &rightPageHandle));

    if (ctx_out != NULL) {
        ctx_out->leftPageNum = leftPageNum;
        ctx_out->rightPageNum = rightPageNum;
        ctx_out->isTargetLeft = targetSlotIdx < numLeftFill;
        ctx_out->targetSlot =
                isTargetLeft
                ? targetSlotIdx
                : targetSlotIdx - numLeftFill;
    }

    return RC_OK;
}


RC IM_insertSplitInner_byAllocLeftRight(
        RM_Page *oldPage,
        int32_t keyValue,
        IM_ENTRY_FORMAT_T *insertEntry,
        uint16_t insertEntryDescriptorSize,
        IM_ENTRY_FORMAT_T *yankedEntry_out,
        BM_BufferPool *pool,
        uint16_t maxEntriesPerNode,
        IM_SplitLeafNodeCtx *ctx_out)
{
    PANIC_IF_NULL(yankedEntry_out);

    if (insertEntryDescriptorSize % sizeof(BF_MessageElement) != 0) {
        PANIC("expected 'entrySize' to be a multiple of 'sizeof(BF_MessageElement)`. "
              "make sure that you using the in-memory 'sizeof(entry)' NOT 'BF_recomputeSize()'");
    }

    IM_ENTRY_FORMAT_T yankedEntry = {0};
    BP_Metadata *meta = pool->mgmtData;
    RM_PageHeader *pageHeader = &oldPage->header;
    const uint16_t initialNumEntries = pageHeader->numTuples;

    // Determine the index at which the entry would normally be inserted
    uint16_t targetSlotIdx = IM_getEntryInsertionIndex(keyValue, oldPage, maxEntriesPerNode);

    // Allocate the left and right leaf nodes
    int leftPageNum = meta->fileHandle->totalNumPages;
    int rightPageNum = leftPageNum + 1;

    BM_PageHandle leftPageHandle = {};
    BM_PageHandle rightPageHandle = {};
    TRY_OR_RETURN(pinPage(pool, &leftPageHandle, leftPageNum));
    TRY_OR_RETURN(pinPage(pool, &rightPageHandle, rightPageNum));

    // Initialize pages
    RM_Page *leftPage = RM_Page_init(leftPageHandle.buffer, leftPageNum, RM_PAGE_KIND_INDEX);
    RM_Page *rightPage = RM_Page_init(rightPageHandle.buffer, rightPageNum, RM_PAGE_KIND_INDEX);

    // Unset leaf flag from the old now inner node
    UNSET_FLAG(oldPage->header.flags, RM_PAGE_FLAGS_INDEX_LEAF);
    SET_FLAG(oldPage->header.flags, RM_PAGE_FLAGS_INDEX_INNER);

    // Set inner node indexing flags
    UNSET_FLAG(leftPage->header.flags, RM_PAGE_FLAGS_INDEX_LEAF);
    UNSET_FLAG(rightPage->header.flags, RM_PAGE_FLAGS_INDEX_LEAF);

    SET_FLAG(leftPage->header.flags, RM_PAGE_FLAGS_INDEX_INNER);
    SET_FLAG(rightPage->header.flags, RM_PAGE_FLAGS_INDEX_INNER);
    
    // Setup the links for the new pages
    //
    // NOTE: We cannot determine the left page end pointer ("nextPageNum")
    // until we determine which entry will be yanked
    //
    RM_PageNumber oldPageNext = oldPage->header.nextPageNum;
    leftPage->header.nextPageNum = RM_PAGE_NEXT_PAGENUM_INVALID;
    rightPage->header.nextPageNum = oldPageNext;

    // Check if the max number of entries is even.
    // If so, the left node gets the extra entry.
    uint16_t numLeftFill = 0;
    bool isMaxEven = maxEntriesPerNode % 2 == 0;
    if (isMaxEven) {
        // ex: if the max == 2, and N = 2 (we exclude the pulled node),
        // then 1 nodes left, 1 node right
        numLeftFill = maxEntriesPerNode / 2;
    } else {
        // ex: if the max == 3, and N = 3 (we exclude the pulled node),
        // then 2 nodes left, 1 nodes right
        numLeftFill = (maxEntriesPerNode + 1) / 2;
    }

    // Determine the index that will be "pulled" to be inserted into its parent
    uint16_t pulledAbsIdx = maxEntriesPerNode;

    // Fill the left until it's "full"
    int oldPageOff = 0; // use when we pass the insertion point, to offset items
    uint16_t i;
    for (i = 0; i < numLeftFill; i++) {
        if (i == targetSlotIdx && i != pulledAbsIdx) {
            // This is where the node we're inserting will go
            //
            // Decrement the page shift by one since we didn't read from the
            // old page
            oldPageOff--;

            // Write out new entry
            RM_PageSlotLength len = insertEntryDescriptorSize;
            RM_PageTuple *newTup = RM_Page_reserveTupleAtEnd(leftPage, len);
            IM_writeEntry_i32(newTup, insertEntry);
        }
        else if (i == targetSlotIdx && i == pulledAbsIdx) {
            // The node that we're trying to insert is also the node that
            // is getting yanked. Copy it to the output pointer.
            yankedEntry = *insertEntry;

            // Decrement the page shift by one since we didn't read from the
            // old page
            oldPageOff--;
        }
        else if (i != targetSlotIdx && i != pulledAbsIdx) {
            // Get the old node
            RM_PageTuple *oldTup = RM_Page_getTuple(oldPage, i + oldPageOff, NULL);
            RM_PageSlotLength len = oldTup->len;

            // Reserve the tuple in the new node to copy the old one
            RM_PageTuple *newTup = RM_Page_reserveTupleAtEnd(leftPage, len);

            // Copy the entry over
            memcpy(&newTup->dataBegin, &oldTup->dataBegin, len);
        }
        else if (i != targetSlotIdx && i == pulledAbsIdx) {
            // Copy old entry to the pulled entry
            RM_PageTuple *oldTup = RM_Page_getTuple(oldPage, i + oldPageOff, NULL);
            RM_PageSlotLength len = oldTup->len;
            IM_readEntry_i32(oldTup, &yankedEntry);
        }
        else {
            PANIC("should never be reached");
        }
    }

    // Fill the rest of the tuples into the right node
    uint16_t maxSlotEntries = initialNumEntries + 1; // increase the skipped over insert
    for (i = numLeftFill; i < maxSlotEntries; i++) {
        if (i == targetSlotIdx && i != pulledAbsIdx) {
            // Write out new entry
            RM_PageSlotLength len = insertEntryDescriptorSize;
            RM_PageTuple *newTup = RM_Page_reserveTupleAtEnd(rightPage, len);
            IM_writeEntry_i32(newTup, insertEntry);
        }
        else if (i == targetSlotIdx && i == pulledAbsIdx) {
            // The node that we're trying to insert is also the node that
            // is getting yanked. Copy it to the output pointer.
            yankedEntry = *insertEntry;

            // Decrement the page shift by one since we didn't read from the
            // old page
            oldPageOff--;
        }
        else if (i != targetSlotIdx && i != pulledAbsIdx) {
            // Get the old node
            RM_PageTuple *oldTup = RM_Page_getTuple(oldPage, i + oldPageOff, NULL);
            RM_PageSlotLength len = oldTup->len;

            // Reserve the tuple in the new node to copy the old one
            RM_PageTuple *newTup = RM_Page_reserveTupleAtEnd(rightPage, len);

            // Copy the entry over
            memcpy(&newTup->dataBegin, &oldTup->dataBegin, len);
        }
        else if (i != targetSlotIdx && i == pulledAbsIdx) {
            // Copy this entry into the pulled entry

            // Copy old entry to the pulled entry
            RM_PageTuple *oldTup = RM_Page_getTuple(oldPage, i + oldPageOff, NULL);
            RM_PageSlotLength len = oldTup->len;
            IM_readEntry_i32(oldTup, &yankedEntry);

            // We don't need to mess with any indexing since the next call
            // to `reserveTupleAtEnd` will simple write to the next location
        }
        else {
            PANIC("should never be reached");
        }
    }
    
    // The yanked entry should be set by now (since it should always get chosen
    // from the right node)
    IM_ENTRY_FORMAT_T emptyEntry = {0};
    if (memcmp(&yankedEntry, &emptyEntry, sizeof(IM_ENTRY_FORMAT_T)) == 0) {
        PANIC("expected yanked entry to be set at this point");
    }

    // Set the left node's end pointer to the yanked entry's pointer
    leftPage->header.nextPageNum = BF_AS_U16(yankedEntry.idxEntryRidPageNum);

    // Clear all nodes on the old page and reset storage flags
    RM_Page_deleteAllTuples(oldPage);

    TRY_OR_RETURN(forcePage(pool, &leftPageHandle));
    TRY_OR_RETURN(forcePage(pool, &rightPageHandle));

    TRY_OR_RETURN(unpinPage(pool, &leftPageHandle));
    TRY_OR_RETURN(unpinPage(pool, &rightPageHandle));

    bool isTargetLeft = targetSlotIdx < numLeftFill;
    if (ctx_out != NULL) {
        ctx_out->leftPageNum = leftPageNum;
        ctx_out->rightPageNum = rightPageNum;
        ctx_out->isTargetLeft = isTargetLeft;
        ctx_out->targetSlot =
                isTargetLeft
                ? targetSlotIdx
                : targetSlotIdx - numLeftFill;
    }

    *yankedEntry_out = yankedEntry;
    return RC_OK;
}


RC IM_insertSplitInner_byReuseLeftAllocRight(
        RM_Page *oldPage,
        int32_t keyValue,
        IM_ENTRY_FORMAT_T *insertEntry,
        uint16_t entryDescriptorSize,
        IM_ENTRY_FORMAT_T *yankedEntry_out,
        BM_BufferPool *pool,
        uint16_t maxEntriesPerNode,
        IM_SplitLeafNodeCtx *ctx_out)
{
    PANIC_IF_NULL(yankedEntry_out);

    if (maxEntriesPerNode == 0) { NOT_IMPLEMENTED(); }

    if (entryDescriptorSize % sizeof(BF_MessageElement) != 0) {
        PANIC("expected 'entrySize' to be a multiple of 'sizeof(BF_MessageElement)`. "
              "make sure that you using the in-memory 'sizeof(entry)' NOT 'BF_recomputeSize()'");
    }

    BP_Metadata *meta = pool->mgmtData;
    RM_PageHeader *pageHeader = &oldPage->header;
    const uint16_t initialNumEntries = pageHeader->numTuples;

    const RM_PageSlotLength entryPhysSize = BF_recomputePhysicalSize(
            (BF_MessageElement *) insertEntry,
            BF_NUM_ELEMENTS(entryDescriptorSize));

    if (entryPhysSize == 0) {
        PANIC("expected 'entryPhysSize' to be > 0");
    }

    // Determine the index at which the entry would normally be inserted
    uint16_t targetSlotIdx = IM_getEntryInsertionIndex(keyValue, oldPage, maxEntriesPerNode);

    // Allocate a new node for the *right* leaf
    // We will reuse the old page as the *left* leaf node since then we can
    // avoid messing with a previous sibling leaf node's next pointer.
    int leftPageNum = oldPage->header.pageNum;
    int rightPageNum = meta->fileHandle->totalNumPages;

    BM_PageHandle rightPageHandle = {};
    TRY_OR_RETURN(pinPage(pool, &rightPageHandle, rightPageNum));

    // Set the *left* leaf page to the old page
    // Initialize new *right* leaf page
    RM_Page *leftPage = oldPage;
    RM_Page *rightPage = RM_Page_init(rightPageHandle.buffer, rightPageNum, RM_PAGE_KIND_INDEX);

    // Set indexing flags
    UNSET_FLAG(leftPage->header.flags, RM_PAGE_FLAGS_INDEX_LEAF);
    UNSET_FLAG(rightPage->header.flags, RM_PAGE_FLAGS_INDEX_LEAF);
    SET_FLAG(leftPage->header.flags, RM_PAGE_FLAGS_INDEX_INNER);
    SET_FLAG(rightPage->header.flags, RM_PAGE_FLAGS_INDEX_INNER);

    // Setup the links for the new pages
    //
    // NOTE: We cannot determine the left page end pointer ("nextPageNum")
    // until we determine which entry will be yanked
    //
    RM_PageNumber oldPageNext = oldPage->header.nextPageNum;
    leftPage->header.nextPageNum = RM_PAGE_NEXT_PAGENUM_INVALID;
    rightPage->header.nextPageNum = oldPageNext;

    // Check if the max number of entries is even.
    bool isMaxEven = maxEntriesPerNode % 2 == 0;

    // ex: if the max == 4, and N = 4, then 2 nodes left, 2 node right
    // ex: if the max == 5, and N = 5, then 3 nodes left, 2 nodes right
    uint16_t numRightFill = maxEntriesPerNode / 2;

    uint16_t numLeftFill = oldPage->header.numTuples - numRightFill;
    uint16_t firstRightAbsSlotId = numLeftFill;
    uint16_t lastAbsSlotId = oldPage->header.numTuples - 1;
    bool isTargetLeft = targetSlotIdx < numLeftFill;

    // Determine the index that will be "pulled" to be inserted into its parent
    uint16_t pulledAbsIdx = maxEntriesPerNode;

    //
    // We're going to work backwards so that we don't override data.
    // Hence we first will need to move over anything that belongs in the
    // *new* right node.
    //

    IM_ENTRY_FORMAT_T yankedEntry = {0};

    //
    // Write out the right node
    //

    uint16_t i;
    uint16_t oldPageOff = isTargetLeft ? 1 : 0;
    for (i = firstRightAbsSlotId; i <= lastAbsSlotId; i++) {
        if (isTargetLeft && i == targetSlotIdx) { PANIC("invalid state"); }

        // Cases where we don't write the entry
        if (i == targetSlotIdx && i == pulledAbsIdx) {
            // If the item we're inserting is also the slot that is getting
            // "yanked", we should skip slot and assign the insertion entry
            // to the pulled entry
            yankedEntry = *insertEntry;

            // Decrement the page shift by one since we didn't read from the
            // old page
            oldPageOff--;

            // Skip writing this entry
            continue;
        }
        else if (i != targetSlotIdx && i == pulledAbsIdx) {
            // We are yanking this entry from the old page
            RM_PageTuple *tup = RM_Page_getTuple(oldPage, i + oldPageOff, NULL);
            IM_readEntry_i32(tup, &yankedEntry);

            // Skip writing this entry
            continue;
        }

        // Cases where we actual write the entry
        RM_PageTuple *oldTup = NULL;
        uint16_t len;
        if (i == targetSlotIdx && i != pulledAbsIdx) {
            // If we hit the position at which we would insert the target,
            // use the entry length
            len = entryPhysSize;
        }
        else if (i != targetSlotIdx && i != pulledAbsIdx) {
            // Otherwise, get the old tuple length
            oldTup = RM_Page_getTuple(oldPage, i + oldPageOff, NULL);
            len = oldTup->len;
        }
        else {
            PANIC("should never be reached");
        }

        RM_PageTuple *newTup = RM_Page_reserveTupleAtEnd(rightPage, len);

        if (i == targetSlotIdx) {
            // Copy the insertion entry into the current tuple
            IM_writeEntry_i32(newTup, insertEntry);

            // Decrement the page shift by one since we didn't read from the
            // old page
            oldPageOff--;
        }
        else {
            // Copy the old tuple to the new *right* tuple
            //
            // NOTE: Since we're writing into the new *right* node
            // left-to-right, we can be sure that we are not overwriting any
            // tuples
            memcpy(&newTup->dataBegin, &oldTup->dataBegin, len);
        }
    }

    //
    // Adjust the old *left* node
    //

    oldPageOff = 0; // reset the offset
    if (targetSlotIdx == numLeftFill - 1) {
        // We're inserting the target node in the last position in the left node
        // In this case, we just need to overwrite the last entry
        RM_PageTuple *tup = RM_Page_getTuple(oldPage, targetSlotIdx, NULL);
        IM_writeEntry_i32(tup, insertEntry);
    }
    else if (targetSlotIdx < numLeftFill - 1) {
        // We're inserting the target node before the last element in the
        // new *left* node. We're going to have to shift the slot pointers in
        // the left node over by one to the right.

        // Delete the last element first to prevent an overflow at insert time
        RM_Page_deleteTuple(oldPage, numLeftFill - 1);

        // Insert the tuple, by shifting the rest of the entries over after it
        RM_PageTuple *tup = RM_Page_reserveTupleAtIndex(oldPage, targetSlotIdx, entryPhysSize);
        IM_writeEntry_i32(tup, insertEntry);
    }
    
    // The yanked entry should be set by now (since it should always get chosen
    // from the right node)
    IM_ENTRY_FORMAT_T emptyEntry = {0};
    if (memcmp(&yankedEntry, &emptyEntry, sizeof(IM_ENTRY_FORMAT_T)) == 0) {
        PANIC("expected yanked entry to be set at this point");
    }

    // Set the left node's end pointer to the yanked entry's pointer
    leftPage->header.nextPageNum = BF_AS_U16(yankedEntry.idxEntryRidPageNum);

    TRY_OR_RETURN(forcePage(pool, &rightPageHandle));
    TRY_OR_RETURN(unpinPage(pool, &rightPageHandle));
    
    if (ctx_out != NULL) {
        ctx_out->leftPageNum = leftPageNum;
        ctx_out->rightPageNum = rightPageNum;
        ctx_out->isTargetLeft = targetSlotIdx < numLeftFill;
        ctx_out->targetSlot =
                isTargetLeft
                ? targetSlotIdx
                : targetSlotIdx - numLeftFill;
    }

    *yankedEntry_out = yankedEntry;
    return RC_OK;
}


RC IM_insertSplit(
        IM_InsertSplitMode mode,
        RM_Page *oldPage,
        int32_t keyValue,
        IM_ENTRY_FORMAT_T *entry,
        uint16_t entryDescriptorSize,
        BM_BufferPool *pool,
        uint16_t maxEntriesPerNode,
        IM_SplitLeafNodeCtx *ctx_out)
{
    switch (mode) {
        case IM_INSERTSPLITMODE_ALLOC_LEFT_RIGHT:
            return IM_insertSplit_byAllocLeftRight(
                    oldPage,
                    keyValue,
                    entry,
                    entryDescriptorSize,
                    pool,
                    maxEntriesPerNode,
                    ctx_out);

        case IM_INSERTSPLITMODE_REUSE_LEFT_ALLOC_RIGHT:
            return IM_insertSplit_byReuseLeftAllocRight(
                    oldPage,
                    keyValue,
                    entry,
                    entryDescriptorSize,
                    pool,
                    maxEntriesPerNode,
                    ctx_out);

        default:
            PANIC("invalid 'mode'");
    }
}

RC IM_insertSplitInner(
        IM_InsertSplitMode mode,
        RM_Page *oldPage,
        int32_t keyValue,
        IM_ENTRY_FORMAT_T *insertEntry,
        uint16_t entryDescriptorSize,
        IM_ENTRY_FORMAT_T *pulledEntry,
        BM_BufferPool *pool,
        uint16_t maxEntriesPerNode,
        IM_SplitLeafNodeCtx *ctx_out)
{
    switch (mode) {
        case IM_INSERTSPLITMODE_ALLOC_LEFT_RIGHT:
            return IM_insertSplitInner_byAllocLeftRight(
                    oldPage,
                    keyValue,
                    insertEntry,
                    entryDescriptorSize,
                    pulledEntry,
                    pool,
                    maxEntriesPerNode,
                    ctx_out);

        case IM_INSERTSPLITMODE_REUSE_LEFT_ALLOC_RIGHT:
            return IM_insertSplitInner_byReuseLeftAllocRight(
                    oldPage,
                    keyValue,
                    insertEntry,
                    entryDescriptorSize,
                    pulledEntry,
                    pool,
                    maxEntriesPerNode,
                    ctx_out);

        default:
            PANIC("invalid 'mode'");
    }
}

RC IM_insertLinkKey_i32(
        BM_BufferPool *pool,
        RM_PageNumber parentPageNum,
        int32_t linkEntryKey,
        RM_PageNumber leftNodePageNum,
        RM_PageNumber rightNodePageNum,
        uint16_t maxEntriesPerNode,
        bool *didInsert_out,
        IM_ENTRY_FORMAT_T *linkEntry_out)
{
    PANIC_IF_NULL(pool);
    PANIC_IF_NULL(didInsert_out);

    // Make entry that links to *left* node using
    // the key value of the *right* node's first entry
    RID linkEntryPtrRid = {
            .page = leftNodePageNum,
            .slot = 0, /* ignored in inner nodes */
    };

    IM_ENTRY_FORMAT_T linkEntry;
    IM_makeEntry_i32(&linkEntry, linkEntryKey, linkEntryPtrRid);
    if (linkEntry_out != NULL) {
        *linkEntry_out = linkEntry;
    }

    uint16_t linkEntryPhysSize = BF_recomputePhysicalSize(
            (BF_MessageElement *) &linkEntry,
            BF_NUM_ELEMENTS(sizeof(linkEntry)));

    // Check if the parent inner node has enough space to insert the link tuple
    // to the right page. Otherwise, we're going to have to split the inner node
    // (ex)
    //  [1,3,4,5]  ==>   |* 4|*| ~ (inserted into parent)
    //    n = 3          /     \
    //                 [1,3]   [4, 5]  (leaf nodes)
    //

    BM_PageHandle parentPageHandle;
    TRY_OR_RETURN(pinPage(pool, &parentPageHandle, parentPageNum));
    RM_Page *parentPage = (RM_Page *) parentPageHandle.buffer;

    if (!IS_FLAG_SET(parentPage->header.flags, RM_PAGE_FLAGS_INDEX_INNER)) {
        PANIC("expected parent page num (%d) to be marked as an inner node", parentPageNum);
    }

    uint16_t parentNumTuples = parentPage->header.numTuples;
    if (parentNumTuples < maxEntriesPerNode)
    {
        // Parent has enough space to add link to the left leaf node
        uint16_t slotId = IM_getEntryInsertionIndex(linkEntryKey, parentPage, maxEntriesPerNode);
        if (slotId < parentNumTuples) {
            // Insert the link at the current location as a tuple
            RM_PageTuple *linkTup = RM_Page_reserveTupleAtIndex(
                    parentPage,
                    slotId,
                    linkEntryPhysSize);

            // Write out the link tuple
            IM_writeEntry_i32(linkTup, &linkEntry);

            // Update the pointer of the next entry to the right node
            uint16_t nextSlotId = slotId + 1;
            RID nextTupRid = {.page = parentPageNum, .slot = nextSlotId};

            IM_ENTRY_FORMAT_T nextEntry;
            IM_readEntryAt_i32(pool, nextTupRid, &nextEntry);

            BF_SET_U16(nextEntry.idxEntryRidPageNum) = rightNodePageNum;
            BF_SET_U16(nextEntry.idxEntryRidSlot) = 0;

            IM_writeEntryAt_i32(pool, nextTupRid, &nextEntry);
        }
        else if (slotId == parentNumTuples) {
            // This branch additionally holds if the inner node has not yet been
            // initialized since slotId == 0 and parentNumTuples == 0
            //

            // Insert the new entry at the end
            RM_PageTuple *linkTup = RM_Page_reserveTupleAtEnd(
                    parentPage,
                    linkEntryPhysSize);

            // Write out the link tuple
            IM_writeEntry_i32(linkTup, &linkEntry);

            // Set the next page num to what we wanted to insert
            // in the first place. If the node was not initialized, then we just
            // set the right-most pointer
            parentPage->header.nextPageNum = rightNodePageNum;
        }
        else {
            PANIC("'slotId' should never be > 'parentNumTuples'");
        }

        TRY_OR_RETURN(markDirty(pool, &parentPageHandle));
        TRY_OR_RETURN(unpinPage(pool, &parentPageHandle));

        *didInsert_out = true;
        return RC_OK;
    }

    TRY_OR_RETURN(unpinPage(pool, &parentPageHandle));
    *didInsert_out = false;
    return RC_OK;
}

RC IM_insertKey_i32(
        BM_BufferPool *pool,
        IM_IndexMetadata *indexMeta,
        int32_t keyValue,
        RID rid)
{
    PANIC_IF_NULL(pool);
    PANIC_IF_NULL(indexMeta);
    LOG_DEBUG( "insert | key = %d, ptr = [%d.%d] |",
            keyValue,
            rid.page,
            rid.slot);

    RC rc;

    //
    // Create the entry node for usage later
    //

    IM_ENTRY_FORMAT_T entry;
    IM_makeEntry_i32(&entry, keyValue, rid);

    const uint16_t entryDiskSize = BF_recomputePhysicalSize(
            (BF_MessageElement *) &entry,
            BF_NUM_ELEMENTS(sizeof(entry)));

    //
    // Find the leaf node to insert into starting at the root node
    //

    const RM_PageNumber rootPageNum = indexMeta->rootNodePageNum;
    const uint16_t maxEntriesPerNode = indexMeta->maxEntriesPerNode;

    // Find the leaf node where we need to insert the entry
    IM_NodeTrace *parents = NULL;
    RID parentRid;
    RM_PageNumber leafNodePageNum = IM_getLeafNode(
            pool,
            rootPageNum,
            keyValue,
            maxEntriesPerNode,
            &parentRid,
            &parents);

    uint16_t parentPageNum = parentRid.page;

    // Calculate initial number of entries in the leaf node
    BM_PageHandle leafNodePageHandle = {};
    TRY_OR_RETURN(pinPage(pool, &leafNodePageHandle, leafNodePageNum));
    RM_Page *oldLeafNodePage = (RM_Page *) leafNodePageHandle.buffer;
    uint16_t initialNumEntries = oldLeafNodePage->header.numTuples;

    // HACK: We're going to assume that `maxEntriesPerNode` is <= max number of tuples
    //       that can fit in a page
    bool hasSpace = initialNumEntries < maxEntriesPerNode;
    if (hasSpace) {
        RM_PageTuple *targetTup = NULL;
        if (initialNumEntries == 0) {
            // Just reserve a new tuple since the node is empty (must be a new root node)
            targetTup = RM_Page_reserveTupleAtEnd(oldLeafNodePage, entryDiskSize);
        }
        else { // if (initialNumEntries > 0)
            // Determine where we need to insert the entry (using linear search)
            uint16_t slotNum = IM_getEntryInsertionIndex(keyValue, oldLeafNodePage, maxEntriesPerNode);
            targetTup = RM_Page_reserveTupleAtIndex(
                    oldLeafNodePage,
                    slotNum,
                    entryDiskSize);
        }

        IM_writeEntry_i32(targetTup, &entry);

        rc = RC_OK;
        goto finally;
    }

    // We don't have enough space and will need to split the leaf node
    //
    // We will need to turn the current node into a inner node
    // and copy the leaf entries into two separate nodes
    //

    // EDGE CASE: if we're splitting the root node as a leaf node,
    // we need to allocate the new left and right leaf nodes so that we can keep
    // the existing root node as a new inner node
    IM_SplitLeafNodeCtx leafSplit = {};
    IM_InsertSplitMode insertMode;
    if (IS_FLAG_SET(oldLeafNodePage->header.flags, RM_PAGE_FLAGS_INDEX_ROOT)) {
        insertMode = IM_INSERTSPLITMODE_ALLOC_LEFT_RIGHT;
    } else {
        insertMode = IM_INSERTSPLITMODE_REUSE_LEFT_ALLOC_RIGHT;
    }

    rc = IM_insertSplit(
            insertMode,
            oldLeafNodePage,
            keyValue,
            &entry,
            sizeof(entry),
            pool,
            maxEntriesPerNode,
            &leafSplit);

    if (rc != RC_OK) { PANIC("failed to split node: got rc = %d", rc); }

    // We then will need to link:
    //  * the new root entry pointer to the left node
    //  * the next page pointer to the right node

    // Get the key value of the first element in the right node
    RID rightPageFirstEntryRid = {
            .page = leafSplit.rightPageNum,
            .slot = 0};

    IM_ENTRY_FORMAT_T rightPageFirstEntry;
    IM_readEntryAt_i32(pool, rightPageFirstEntryRid, &rightPageFirstEntry);
    int32_t linkEntryKey = BF_AS_I32(rightPageFirstEntry.idxEntryKey);

    bool didInsertLinkKey = false;
    IM_ENTRY_FORMAT_T linkEntry;
    TRY_OR_RETURN(IM_insertLinkKey_i32(
            pool,
            parentPageNum,
            linkEntryKey,
            leafSplit.leftPageNum,
            leafSplit.rightPageNum,
            maxEntriesPerNode,
            &didInsertLinkKey,
            &linkEntry));
    
    if (didInsertLinkKey) {
        rc = RC_OK;
        goto finally;
    }

    BM_PageHandle parentPageHandle = {};
    RM_Page *parentPage;

    int traceParentIdx = parents->entryLen - 1;
    int traceGrandParentIdx;

    do {
        // Open parent page
        TRY_OR_RETURN(pinPage(pool, &parentPageHandle, parentPageNum));
        parentPage = (RM_Page *) parentPageHandle.buffer;
        traceGrandParentIdx = traceParentIdx - 1;

        // Parent inner node is full, we're going to need to split it and perform
        // an inner node split
        //
        // (ex)
        //  [1,3,4,5]  ==>   |* 4|*| ~ (inserted into parent)
        //    n = 3          /     \
        //  / / /  \      [1,3]   [5]  (inner nodes)
        //

        // Determine if the parent inner node is the root node or not
        IM_InsertSplitMode parentMode;
        if (IS_FLAG_SET(parentPage->header.flags, RM_PAGE_FLAGS_INDEX_ROOT)) {
            parentMode = IM_INSERTSPLITMODE_ALLOC_LEFT_RIGHT;
        }
        else {
            parentMode = IM_INSERTSPLITMODE_REUSE_LEFT_ALLOC_RIGHT;
        }

        IM_SplitLeafNodeCtx parentSplit;
        IM_ENTRY_FORMAT_T parentYankedEntry;
        TRY_OR_RETURN(IM_insertSplitInner(
                parentMode,
                parentPage,
                linkEntryKey,
                &linkEntry,
                sizeof(linkEntry),
                &parentYankedEntry,
                pool,
                maxEntriesPerNode,
                &parentSplit));

        // Recompute physical size of the yanked entry
        uint16_t parentYankedEntryPhysSize = BF_recomputePhysicalSize(
                (BF_MessageElement *) &parentYankedEntry,
                BF_NUM_ELEMENTS(sizeof(parentYankedEntry)));

        // Attempt to insert the yanked entry into the parent's parent
        RM_PageNumber grantParentPageNum;
        if (traceParentIdx == 0) {
            // The parent node is already the root node
            // We are garenteed to have space since we must have called the insert
            // with "alloc left right" thereby removing all entries from the parent
            grantParentPageNum = parentPageNum;

            if (parentPageNum != parents->entryArr[traceParentIdx].page) {
                PANIC("consistency check failed");
            }
        }
        else {
            grantParentPageNum = parents->entryArr[traceGrandParentIdx].page;
        }

        int32_t parentYankedEntryKey = BF_AS_I32(parentYankedEntry.idxEntryKey);

        bool didInsertParentLinkKey = false;
        IM_ENTRY_FORMAT_T parentLinkEntry;
        TRY_OR_RETURN(IM_insertLinkKey_i32(
                pool,
                grantParentPageNum,
                parentYankedEntryKey,
                parentSplit.leftPageNum,
                parentSplit.rightPageNum,
                maxEntriesPerNode,
                &didInsertParentLinkKey,
                &parentLinkEntry));

        TRY_OR_RETURN(markDirty(pool, &parentPageHandle));
        TRY_OR_RETURN(unpinPage(pool, &parentPageHandle));

        if (didInsertParentLinkKey) {
            rc = RC_OK;
            goto finally;
        }
        else {
            if (traceParentIdx == 0) {
                PANIC("inserting link key into fresh root node should not fail");
            }

            // Setup next iteration
            traceParentIdx--;
            parentPageNum = grantParentPageNum;
        }

    } while (true);

    PANIC("should never be reached");

finally:
    IM_NodeTrace_free(parents);
    TRY_OR_RETURN(forcePage(pool, &leafNodePageHandle));
    TRY_OR_RETURN(unpinPage(pool, &leafNodePageHandle));
    return rc;
}

RM_PageNumber
IM_getLeafNode(
        BM_BufferPool *pool,
        RM_PageNumber rootPageNum,
        int32_t searchKey,
        uint16_t maxEntriesPerNode,
        RID *parent_out_opt,
        IM_NodeTrace **trace_out_opt)
{
    PANIC_IF_NULL(pool);

    BM_PageHandle parentNodeHandle = {};
    RM_Page *parentNode = NULL;

    BM_PageHandle nodeHandle = {};
    RM_Page *node = NULL;

    IM_NodeTrace *trace = NULL;
    if (trace_out_opt != NULL) {
        trace = malloc(sizeof(IM_NodeTrace));
        trace->entryLen = 0;
        trace->entryCapacity = IM_NODETRACE_INITIAL_CAPACITY;
        trace->entryArr = calloc(IM_NODETRACE_INITIAL_CAPACITY, sizeof(*trace->entryArr));
    }

    RID parentRid;
    RM_PageNumber nodePageNum = rootPageNum;
    do {
        // Pin the next node page
        if (pinPage(pool, &nodeHandle, nodePageNum) != RC_OK) {
            PANIC("failed to get node page '%d'", nodePageNum);
        }
        node = (RM_Page *) nodeHandle.buffer;

        // Ensure that we are reading from a index page
        if (node->header.kind != RM_PAGE_KIND_INDEX) {
            PANIC("attempted to read from wrong page kind. expected index page.");
        }

        // If the node is still a leaf node, then we don't have to go any further
        if (IS_FLAG_SET(node->header.flags, RM_PAGE_FLAGS_INDEX_LEAF)) {
            if (unpinPage(pool, &nodeHandle) != RC_OK) {
                PANIC("failed to unpin node page");
            }

            break;
        }

        // Otherwise, the current node must be an internal node, a RID pointer
        // will point to the child node.
        //
        // Find the index that is less than or equal to the search key
        //
        uint16_t slotId = IM_getEntryIndexByPredicate(
                searchKey,
                node,
                IM_GETENTRY_OP_LT,
                maxEntriesPerNode,
                NULL,
                NULL);

        parentRid.page = nodePageNum;
        parentRid.slot = slotId;

        // If tracing is enabled, append to the list
        if (trace != NULL) {
            IM_NodeTrace_append(trace, parentRid);
        }

        //
        // Determine the next page number
        //

        // Check if the slot index == numTuples,
        //   if so, we need to use the `page.header->nextPageNum` pointer as the
        //   right-most pointer on the node to get the next page
        //
        uint16_t nodeNumTuples = node->header.numTuples;
        if (slotId == nodeNumTuples) {
            nodePageNum = node->header.nextPageNum;
        }
        else if (slotId < nodeNumTuples) {
            // Traverse to the newly selected node
            RM_PageTuple *nextTup = RM_Page_getTuple(node, slotId, NULL);

            // Determine where the next page is located
            IM_ENTRY_FORMAT_T entry;
            IM_readEntry_i32(nextTup, &entry);
            nodePageNum = BF_AS_U16(entry.idxEntryRidPageNum);
        }
        else {
            PANIC("bad 'slotId' value. expected to be <= number of tuples in node");
            nodePageNum = -1;  // stop static analyzer from complaining
        }

        if (nodePageNum == (RM_PageNumber) -1) {
            PANIC("bad 'nextPageNum'. got -1.");
        }

        // Unpin previous parent, if available
        if (parentNode != NULL) {
            if (unpinPage(pool, &parentNodeHandle) != RC_OK) {
                PANIC("failed to unpin parent node page");
            }
            memset(&parentNodeHandle, 0, sizeof(parentNodeHandle));
            parentNode = NULL;
        }

        // Move current page to parent page
        parentNode = node;
        parentNodeHandle = nodeHandle;

    } while (true);

    if (parent_out_opt != NULL) {
        if (nodePageNum == rootPageNum) {
            parent_out_opt->page = rootPageNum;
            parent_out_opt->slot = 0;
        }
        else {
            *parent_out_opt = parentRid;
        }
    }

    if (trace_out_opt != NULL) {
        *trace_out_opt = trace;
    }

    return nodePageNum;
}

static IM_GETENTRY_OP_DECL(EQ, ==)
static IM_GETENTRY_OP_DECL(LT, <)
static IM_GETENTRY_OP_DECL(LE, <=)
static IM_GETENTRY_OP_DECL(GT, >)
static IM_GETENTRY_OP_DECL(GE, >=)

IM_GetEntryOpFn IM_GETENTRY_OP_FN_LOOKUP[IM_GETENTRY_OP_COUNT] = {
        IM_GETENTRY_OP_LOOKUP(EQ),
        IM_GETENTRY_OP_LOOKUP(LT),
        IM_GETENTRY_OP_LOOKUP(LE),
        IM_GETENTRY_OP_LOOKUP(GT),
        IM_GETENTRY_OP_LOOKUP(GE),
};

#define IM_GETENTRY_STR(OP) \
    ((IM_GETENTRY_OP_EQ == (OP)) ? "slot == Q" : \
     (IM_GETENTRY_OP_LT == (OP)) ? "slot < Q"  : \
     (IM_GETENTRY_OP_GT == (OP)) ? "slot > Q"  : \
     (IM_GETENTRY_OP_GE == (OP)) ? "slot >= Q" : \
     (IM_GETENTRY_OP_LE == (OP)) ? "slot <= Q" : "error")

uint16_t
IM_getEntryIndexByPredicate(
        const int32_t keyValue,
        RM_Page *page,
        IM_GetEntryOp op,
        uint16_t maxEntriesPerNode,
        IM_ENTRY_FORMAT_T *entry_out_opt,
        uint16_t *numSlots_out_opt)
{
    uint16_t numTuples = page->header.numTuples;
    uint16_t maxSlots =
            (numTuples < maxEntriesPerNode)
            ? numTuples
            : maxEntriesPerNode;

    if (numSlots_out_opt != NULL) {
        *numSlots_out_opt = maxSlots;
    }

    IM_ENTRY_FORMAT_T markEntry;
    uint16_t slotNum = 0;
    for (; slotNum < maxSlots; slotNum++) {
        // get position of tuple
        RM_PageTuple *markTup = RM_Page_getTuple(page, slotNum, NULL);

        // read in entry data
        IM_readEntry_i32(markTup, &markEntry);

        // stop if predicate matches
        int32_t markKey = BF_AS_I32(markEntry.idxEntryKey);
        IM_GetEntryOpFn fn = IM_GETENTRY_OP_FN_LOOKUP[op];
        if (fn(keyValue, markKey)) {
            if (entry_out_opt != NULL) {
                *entry_out_opt = markEntry;
            }
            break;
        }
    }

    LOG_DEBUG("page num = %d, q = %d, op = %s | @ result idx = %d",
            page->header.pageNum, keyValue, IM_GETENTRY_STR(op), slotNum);

    return slotNum;
}

uint16_t
IM_getEntryInsertionIndex(
        const int32_t keyValue,
        RM_Page *page,
        uint16_t maxEntriesPerNode)
{
    return IM_getEntryIndexByPredicate(
            keyValue,
            page,
            IM_GETENTRY_OP_LT,
            maxEntriesPerNode,
            NULL,
            NULL);
}

bool
IM_getEntryIndex(
        const int32_t keyValue,
        RM_Page *page,
        uint16_t maxEntriesPerNode,
        IM_ENTRY_FORMAT_T *entry_out_opt,
        uint16_t *slotId_out_opt)
{
    uint16_t numSlots = 0;
    uint16_t slotId = IM_getEntryIndexByPredicate(
            keyValue,
            page,
            IM_GETENTRY_OP_EQ,
            maxEntriesPerNode,
            entry_out_opt,
            &numSlots);

    if (slotId_out_opt != NULL) {
        *slotId_out_opt = slotId;
    }

    return slotId < numSlots;
}

RC IM_findEntry_i32(
        BM_BufferPool *pool,
        IM_IndexMetadata *indexMeta,
        int32_t keyValue,
        RID *entryValue_out_opt,
        RID *entryIndex_out_opt,
        RID *parentLinkRid_out_opt)
{
    RC rc;
    const uint16_t maxEntriesPerNode = indexMeta->maxEntriesPerNode;
    RM_PageNumber leafPageNum = IM_getLeafNode(
            pool,
            indexMeta->rootNodePageNum,
            keyValue,
            maxEntriesPerNode,
            parentLinkRid_out_opt,
            NULL);

    BM_PageHandle leafPageHandle;
    TRY_OR_RETURN(pinPage(pool, &leafPageHandle, leafPageNum));
    RM_Page *leafPage = (RM_Page *) leafPageHandle.buffer;

    RM_PageSlotId entrySlotId;
    IM_ENTRY_FORMAT_T entry;
    bool found = IM_getEntryIndex(
            keyValue,
            leafPage,
            maxEntriesPerNode,
            &entry,
            &entrySlotId);

    if (!found) {
        rc = RC_IM_KEY_NOT_FOUND;
        goto finally;
    }

    if (entryIndex_out_opt != NULL) {
        entryIndex_out_opt->page = leafPageNum;
        entryIndex_out_opt->slot = entrySlotId;
    }

    if (entryValue_out_opt != NULL) {
        entryValue_out_opt->page = BF_AS_U16(entry.idxEntryRidPageNum);
        entryValue_out_opt->slot = BF_AS_U16(entry.idxEntryRidSlot);
    }

    rc = RC_OK;

finally:
    TRY_OR_RETURN(unpinPage(pool, &leafPageHandle));
    return rc;
}

RC IM_deleteKey_i32(
        BM_BufferPool *pool,
        IM_IndexMetadata *indexMeta,
        const int32_t keyValue)
{
    PANIC_IF_NULL(pool);
    PANIC_IF_NULL(indexMeta);
    RC rc;

    LOG_DEBUG("key = %d", keyValue);

    RID leafEntryRid;
    RID parentLinkRid;
    TRY_OR_RETURN(IM_findEntry_i32(
            pool,
            indexMeta,
            keyValue,
            NULL,
            &leafEntryRid,
            &parentLinkRid));

    LOG_DEBUG("key = %d, page num = %d, slot id = %d",
            keyValue,
            leafEntryRid.page,
            leafEntryRid.slot);

    BM_PageHandle pageHandle;
    TRY_OR_RETURN(pinPage(pool, &pageHandle, leafEntryRid.page));
    RM_Page *leafPage = (RM_Page *) pageHandle.buffer;

    if (leafPage->header.kind != RM_PAGE_KIND_INDEX) {
        PANIC("expected index page");
    }

    if (!IS_FLAG_SET(leafPage->header.flags, RM_PAGE_FLAGS_INDEX_LEAF)) {
        PANIC("expected page to be a leaf node");
    }

    BM_PageHandle parentPageHandle;
    TRY_OR_RETURN(pinPage(pool, &parentPageHandle, parentLinkRid.page));
    RM_Page *parentPage = (RM_Page *) parentPageHandle.buffer;

    if (parentPage->header.kind != RM_PAGE_KIND_INDEX) {
        PANIC("expected index page");
    }

    //
    // Determine if there will still be items in the leaf node once the entry
    // is removed (i.e. check for underflow)
    //
    uint16_t initialLeafNumTuples = leafPage->header.numTuples;
    if (initialLeafNumTuples == 0) {
        PANIC("expected number of leaf tuples to be greater than 0");
    }

    if (initialLeafNumTuples == 1) {
        //
        // Handle underflow
        //

        // If we're in the root as a leaf, there is nothing to do
        // Simply remove the entry but leave the node
        if (IS_FLAG_SET(leafPage->header.flags, RM_PAGE_FLAGS_INDEX_ROOT)) {
            RM_Page_deleteTupleAtIndex(leafPage, leafEntryRid.slot);
            rc = RC_OK;
            goto finally;
        }

        // Determine which, if any sibling leaf nodes we can borrow from
        uint16_t parentNumTuples = parentPage->header.numTuples;
        uint16_t parentLastSlotId = parentNumTuples - 1;
        bool hasLeftSibling = parentLinkRid.slot > 0;
        bool hasRightSibling = parentLinkRid.slot < (parentLastSlotId - 1);
        
        // Prefer borrowing from the left sibling
        if (hasLeftSibling)
        {
            //
            // Does the left sibling have more than one tuple to allow us to
            // borrow it?
            //
            
            RID leftSiblingLinkRid = {
                    .page = parentLinkRid.page,
                    .slot = parentLinkRid.slot - 1};
            IM_ENTRY_FORMAT_T leftSiblingLinkEntry;
            IM_readEntryAt_i32(pool, leftSiblingLinkRid, &leftSiblingLinkEntry);
            
            // Resolve the sibling leaf page and determine if there's enough
            // entries
            RM_PageNumber leftSiblingPageNum = BF_AS_U16(leftSiblingLinkEntry.idxEntryRidPageNum);

            uint16_t leftSiblingNumTuples;
            RM_Page_getNumTuplesAt(pool, leftSiblingPageNum, &leftSiblingNumTuples);
            
            if (leftSiblingNumTuples > 1) {
                LOG_DEBUG("borrow from left sibling");

                // We have enough in the left sibling to borrow
                // Delete the tuple we're trying to remove
                RM_Page_deleteTuple(leafPage, leafEntryRid.slot);

                uint16_t leftSiblingBorrowedTupIdx = leftSiblingNumTuples - 1;
                RID leftSiblingEntryRid = {
                        .page = leftSiblingPageNum,
                        .slot = leftSiblingBorrowedTupIdx,
                };
                IM_ENTRY_FORMAT_T leftSiblingEntry;
                IM_readEntryAt_i32(pool, leftSiblingEntryRid, &leftSiblingEntry);
                int32_t borrowedValue = BF_AS_I32(leftSiblingEntry.idxEntryKey);

                // Always will insert into the first slot since there are no
                // tuples in the leaf node at this point
                RID insertRid = {
                        .page = leafPage->header.pageNum,
                        .slot = 0};
                RM_Page_moveTuple(pool, leftSiblingEntryRid, insertRid);

                // Fixup left sibling link
                BF_SET_I32(leftSiblingLinkEntry.idxEntryKey) = borrowedValue;
                IM_writeEntryAt_i32(pool, leftSiblingLinkRid, &leftSiblingLinkEntry);

                rc = RC_OK;
                goto finally;
            }
        }

        if (hasRightSibling)
        {
            //
            // Does the right sibling have more than one tuple to allow us to
            // borrow it?
            //

            RID rightSiblingLinkRid = {
                    .page = parentLinkRid.page,
                    .slot = parentLinkRid.slot + 1};

            // Resolve the sibling leaf page and determine if there's enough
            // entries
            RM_PageNumber rightSiblingPageNum;

            if (rightSiblingLinkRid.slot < parentNumTuples) {
                IM_ENTRY_FORMAT_T rightSiblingLinkEntry;
                IM_readEntryAt_i32(pool, rightSiblingLinkRid, &rightSiblingLinkEntry);
                rightSiblingPageNum = BF_AS_U16(rightSiblingLinkEntry.idxEntryRidPageNum);
            }
            else {
                int32_t nextPageNum = parentPage->header.nextPageNum;
                if (nextPageNum == RM_PAGE_NEXT_PAGENUM_UNSET) { PANIC("missing end pointer"); }
                rightSiblingPageNum = (uint16_t) nextPageNum;
            }

            uint16_t rightSiblingNumTuples;
            RM_Page_getNumTuplesAt(pool, rightSiblingPageNum, &rightSiblingNumTuples);

            if (rightSiblingNumTuples > 1) {
                LOG_DEBUG("borrow from right sibling");

                // We have enough in the left sibling to borrow
                // Delete the tuple we're trying to remove
                RM_Page_deleteTuple(leafPage, leafEntryRid.slot);

                RID rightSiblingFirstEntryRid = {
                        .page = rightSiblingPageNum,
                        .slot = 0,
                };
                IM_ENTRY_FORMAT_T rightSiblingEntry;
                IM_readEntryAt_i32(pool, rightSiblingFirstEntryRid, &rightSiblingEntry);
                int32_t borrowedKeyValue = BF_AS_I32(rightSiblingEntry.idxEntryKey);

                // The leaf node is currently empty, we'll always be inserting
                // at the 0th element
                RID insertRid = {
                        .page = leafPage->header.pageNum,
                        .slot = 0};
                RM_Page_moveTuple(pool, rightSiblingFirstEntryRid, insertRid);

                // We've updated the first element in the leaf node, so we need
                // to fixup the link key values
                //
                if (parentLinkRid.slot - 1 >= 0) {
                    //
                    // If the leaf link slot is at least at the 1st position,
                    // we need to update the prior link slot with the borrowed
                    // key value.
                    //
                    IM_ENTRY_FORMAT_T leftSiblingLinkEntry;
                    RID leftSiblingEntryRid = {
                            .page = parentLinkRid.page,
                            .slot = parentLinkRid.slot - 1,
                    };
                    IM_readEntryAt_i32(pool, leftSiblingEntryRid, &leftSiblingLinkEntry);

                    BF_SET_I32(leftSiblingLinkEntry.idxEntryKey) = borrowedKeyValue;
                    IM_writeEntryAt_i32(pool, leftSiblingEntryRid, &leftSiblingLinkEntry);
                }

                // Since there is guaranteed to the a right sibling leaf node
                // exist, we need to update our parent link value to that of the
                // new first entry in the right sibling.

                // Read the parent link entry
                IM_ENTRY_FORMAT_T parentLinkEntry;
                IM_readEntryAt_i32(pool, parentLinkRid, &parentLinkEntry);

                // Get the new value of the right sibling leaf node
                IM_readEntryAt_i32(pool, rightSiblingFirstEntryRid, &rightSiblingEntry);

                // Update the parent link entry to the leaf node
                BF_SET_I32(parentLinkEntry.idxEntryKey) =
                        BF_AS_I32(rightSiblingEntry.idxEntryKey);
                IM_writeEntryAt_i32(pool, parentLinkRid, &rightSiblingEntry);

                rc = RC_OK;
                goto finally;
            }
        }

        //
        // No siblings with extra entries, we have to actually remove the node
        //

        if (parentLinkRid.slot >= parentNumTuples) {
            // If the node is linked to from the end pointer, then we need to
            // remove the previous sibling link and move that pointer to the end

            // Get pointer from previous sibling
            IM_ENTRY_FORMAT_T leftSiblingLinkEntry;
            RID leftSiblingEntryRid = {
                    .page = parentLinkRid.page,
                    .slot = parentLastSlotId,
            };
            IM_readEntryAt_i32(pool, leftSiblingEntryRid, &leftSiblingLinkEntry);

            // Remove previous sibling link
            RM_Page_deleteTupleAtIndex(parentPage, parentLastSlotId);

            // Link back to that node
            parentPage->header.nextPageNum = BF_AS_U16(leftSiblingLinkEntry.idxEntryRidPageNum);
        }
        else { // parentLinkRid.slot < parentNumTuples

            // If the node is linked via slot, simply delete it, shifting the
            // rest over to the left
            RM_Page_deleteTupleAtIndex(parentPage, parentLinkRid.slot);
        }

        // Free page
        RM_Page_free(leafPage);

        // If the parent node underflowed, we have to perform a merge
        if (parentPage->header.numTuples == 0) {
            NOT_IMPLEMENTED();
        }

        rc = RC_OK;
        goto finally;
    }

    // Delete pointer, handling fixing-up the slot pointers
    RM_Page_deleteTupleAtIndex(leafPage, leafEntryRid.slot);

    // Check if the item that is being removed is the first entry
    // If so, we have to update the parent node's entry value to the next
    // sibling's value
    if (leafEntryRid.slot == 0 && parentLinkRid.slot > 0) {
        RM_PageSlotId prevSiblingLinkSlotId = parentLinkRid.slot - 1;
        RID prevLinkEntryRid = {
                .page = parentLinkRid.page,
                .slot = prevSiblingLinkSlotId};

        IM_ENTRY_FORMAT_T prevSiblingLinkEntry;
        IM_readEntryAt_i32(pool, prevLinkEntryRid, &prevSiblingLinkEntry);

        // Get the new first entry in our current leaf node
        // This value will be set to the previous link node key
        RM_PageTuple *leafFirstEntryTup = RM_Page_getTuple(
                leafPage, 0, NULL);
        IM_ENTRY_FORMAT_T leafFirstEntry;
        IM_readEntry_i32(leafFirstEntryTup, &leafFirstEntry);

        BF_SET_I32(prevSiblingLinkEntry.idxEntryKey) =
                BF_AS_I32(leafFirstEntry.idxEntryKey);

        IM_writeEntryAt_i32(pool, prevLinkEntryRid, &prevSiblingLinkEntry);
    }

    rc = RC_OK;

finally:
    TRY_OR_RETURN(markDirty(pool, &pageHandle));
    TRY_OR_RETURN(unpinPage(pool, &pageHandle));

    TRY_OR_RETURN(markDirty(pool, &parentPageHandle));
    TRY_OR_RETURN(unpinPage(pool, &parentPageHandle));
    return rc;
}

RC IM_deleteIndex(BM_BufferPool *pool, char *idxId)
{
    PANIC_IF_NULL(idxId);

    BM_PageHandle systemPageHandle;
    IM_DESCRIPTOR_FORMAT_T indexData;
    RM_PageTuple *indexTup;
    TRY_OR_RETURN(IM_findIndex(
            pool,
            idxId,
            &systemPageHandle,
            &indexTup,
            &indexData));
    RM_Page *systemPage = (RM_Page *) systemPageHandle.buffer;

    IM_IndexMetadata indexMeta;
    IM_IndexMetadata_makeFromMessage(&indexMeta, &indexData);
    RM_PageNumber rootPageNum = indexMeta.rootNodePageNum;

    // We can delete the index descriptor tuple now since we already have the
    // data loaded
    RM_Page_deleteTuple(systemPage, indexTup->slotId);

    // Allocate a stack to push parent nodes when we need to traverse it's
    // children
    uint32_t stackCapacity = 16;
    uint32_t stackLen = 0;
    IM_GetNumNodes_Elem *stack = calloc(stackCapacity, sizeof(*stack));

    RM_PageNumber curPageNum = rootPageNum;
    BM_PageHandle curPageHandle;
    RM_PageSlotId nextSlotId = 0;
    do {
        TRY_OR_RETURN(pinPage(pool, &curPageHandle, curPageNum));
        RM_Page *curPage = (RM_Page *) curPageHandle.buffer;

        bool isCurPageRoot = IS_FLAG_SET(curPage->header.flags, RM_PAGE_FLAGS_INDEX_ROOT);
        bool isCurPageLeaf = IS_FLAG_SET(curPage->header.flags, RM_PAGE_FLAGS_INDEX_LEAF);

        if (isCurPageRoot && isCurPageLeaf) {
            // The index consists of only the root page
            RM_Page_free(curPage);
            break;
        }

        if (isCurPageLeaf) {
            PANIC("expected inner node but got leaf node");
        }

        // Current node is an inner node
        //
        // Determine if this inner node has other inner node children
        // or if it only has leaf nodes as children, since if it's only
        // leaf nodes, we can just use the `numTuples`

        RM_PageTuple *tup = RM_Page_getTuple(curPage, nextSlotId, NULL);
        IM_ENTRY_FORMAT_T entry;
        IM_readEntry_i32(tup, &entry);

        RM_PageNumber slotPageNum = BF_AS_U16(entry.idxEntryRidPageNum);
        BM_PageHandle slotPageHandle;
        TRY_OR_RETURN(pinPage(pool, &slotPageHandle, slotPageNum));
        RM_Page *slotPage = (RM_Page *) slotPageHandle.buffer;

        if (slotPage->header.kind != RM_PAGE_KIND_INDEX) {
            PANIC("expected page referenced by slot of non-leaf node "
                  "to be an index page");
        }

        if (IS_FLAG_SET(slotPage->header.flags, RM_PAGE_FLAGS_INDEX_LEAF)) {
            // The rest of the slots are leaf nodes, delete each of them
            // one by one
            
            // Delete the current slot page
            RM_Page_free(slotPage);
            TRY_OR_RETURN(unpinPage(pool, &slotPageHandle));
            nextSlotId++;

            // Delete the rest of the slot pages
            uint16_t numTuples = curPage->header.numTuples;
            for (; nextSlotId < numTuples; nextSlotId++) {
                RM_PageTuple *leafNodeTup = RM_Page_getTuple(curPage, nextSlotId, NULL);
                IM_ENTRY_FORMAT_T leafNodeEntry;
                IM_readEntry_i32(leafNodeTup, &leafNodeEntry);

                RM_PageNumber leafNodePageNum = BF_AS_U16(leafNodeEntry.idxEntryRidPageNum);
                RM_Page_freeAt(pool, leafNodePageNum);
            }

            // Delete this inner node
            RM_Page_free(curPage);

            // Attempt to pop the parent to read the rest of the parent inner slots
            bool finished = false;
            if (stackLen > 0) {
                struct IM_GetNumNodes_Elem *el = &stack[stackLen - 1];
                curPageNum = el->pageNum;
                nextSlotId = el->nextSlotId;
                stackLen--;
            } else {
                finished = true;
            }

            TRY_OR_RETURN(unpinPage(pool, &slotPageHandle));
            if (finished) {
                break;
            }

            continue;
        }

        // If the first slot is not a leaf, then this inner node must have inner
        // node children. We need to traverse through each slot then.
        //
        // Push the current node to be the parent node and increment the next
        // slot id.
        if (stackLen == stackCapacity) {
            stackCapacity = stackCapacity << 1u;
            stack = realloc(stack, stackCapacity);
        }
        stack[stackLen].pageNum = curPageNum;
        stack[stackLen].nextSlotId = nextSlotId + 1;
        stackLen++;

        // Traverse the first inner node
        curPageNum = slotPageNum;
        nextSlotId = 0;
        TRY_OR_RETURN(unpinPage(pool, &slotPageHandle));

    } while (true);

    free(stack);
    return RC_OK;
}

RC IM_getNumNodes(
        BM_BufferPool *pool,
        IM_IndexMetadata *indexMeta,
        int *result_out)
{
    PANIC_IF_NULL(pool);
    PANIC_IF_NULL(indexMeta);
    PANIC_IF_NULL(result_out);

    RM_PageNumber rootPageNum = indexMeta->rootNodePageNum;

    // Allocate a stack to push parent nodes when we need to traverse it's
    // children
    uint32_t stackCapacity = 16;
    uint32_t stackLen = 0;
    struct IM_GetNumNodes_Elem *stack = calloc(stackCapacity, sizeof(*stack));

    int numNodes = 1;

    RM_PageNumber curPageNum = rootPageNum;
    BM_PageHandle curPageHandle;
    RM_PageSlotId nextSlotId = 0;
    do {
        TRY_OR_RETURN(pinPage(pool, &curPageHandle, curPageNum));
        RM_Page *curPage = (RM_Page *) curPageHandle.buffer;

        bool isCurPageRoot = IS_FLAG_SET(curPage->header.flags, RM_PAGE_FLAGS_INDEX_ROOT);
        bool isCurPageLeaf = IS_FLAG_SET(curPage->header.flags, RM_PAGE_FLAGS_INDEX_LEAF);

        if (isCurPageRoot && isCurPageLeaf) {
            // There is only one node
            break;
        }

        if (isCurPageLeaf) {
            PANIC("expected inner node but got leaf node");
        }

        // Current node is an inner node
        //
        // Determine if this inner node has other inner node children
        // or if it only has leaf nodes as children, since if it's only
        // leaf nodes, we can just use the `numTuples`

        RM_PageTuple *tup = RM_Page_getTuple(curPage, nextSlotId, NULL);
        IM_ENTRY_FORMAT_T entry;
        IM_readEntry_i32(tup, &entry);

        RM_PageNumber slotPageNum = BF_AS_U16(entry.idxEntryRidPageNum);
        BM_PageHandle slotPageHandle;
        TRY_OR_RETURN(pinPage(pool, &slotPageHandle, slotPageNum));
        RM_Page *slotPage = (RM_Page *) slotPageHandle.buffer;

        if (slotPage->header.kind != RM_PAGE_KIND_INDEX) {
            PANIC("expected page referenced by slot of non-leaf node "
                  "to be an index page");
        }

        if (IS_FLAG_SET(slotPage->header.flags, RM_PAGE_FLAGS_INDEX_LEAF)) {
            // The rest of the slots should be leaf nodes, therefore we can
            // just count the slots on this inner node
            // Add +1 for the nextPage pointer
            numNodes += curPage->header.numTuples + 1;

            // Attempt to pop the parent to read the rest of the parent inner slots
            bool finished = false;
            if (stackLen > 0) {
                struct IM_GetNumNodes_Elem *el = &stack[stackLen - 1];
                curPageNum = el->pageNum;
                nextSlotId = el->nextSlotId;
            } else {
                finished = true;
            }

            TRY_OR_RETURN(unpinPage(pool, &slotPageHandle));
            if (finished) {
                break;
            }

            continue;
        }

        // If the first slot is not a leaf, then this inner node must have inner
        // node children. We need to traverse through each slot then.
        //
        // Push the current node to be the parent node and increment the next
        // slot id.
        if (stackLen == stackCapacity) {
            stackCapacity = stackCapacity << 1u;
            stack = realloc(stack, stackCapacity);
        }
        stack[stackLen].pageNum = curPageNum;
        stack[stackLen].nextSlotId = nextSlotId + 1;
        stackLen++;

        // Traverse the first inner node
        curPageNum = slotPageNum;
        nextSlotId = 0;
        TRY_OR_RETURN(unpinPage(pool, &slotPageHandle));

    } while (true);

    free(stack);

    *result_out = numNodes;
    return RC_OK;
}
