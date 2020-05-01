#include <stdlib.h>
#include <string.h>

#include "record_mgr.h"
#include "buffer_mgr.h"
#include "tables.h"
#include "btree.h"
#include "rm_macros.h"
#include "rm_page.h"
#include "btree_binfmt.h"

void IM_NodeTrace_append(IM_NodeTrace *self, RID val)
{
    if (self->entryLen == self->entryCapacity) {
        self->entryCapacity = self->entryCapacity << 1u; // next power of 2
        self->entryArr = realloc(self->entryArr, self->entryCapacity);
    }

    self->entryArr[self->entryLen] = val;
    self->entryLen++;
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

void IM_readEntry_i32(RM_PageTuple *tup, IM_ENTRY_FORMAT_T *result, size_t n)
{
    PANIC_IF_NULL(tup);
    PANIC_IF_NULL(result);
    if (tup->len == 0) { PANIC("length of 'tup' buffer cannot be zero"); }
    if (n == 0) { PANIC("'size' must be greater than zero bytes"); }

    memcpy(result, &IM_ENTRY_FORMAT_OF_I32, sizeof(IM_ENTRY_FORMAT_OF_I32));
    uint16_t read = BF_read(
            (BF_MessageElement *) result,
            &tup->dataBegin,
            BF_NUM_ELEMENTS(n));
    if (read > tup->len) {
        PANIC("buffer overrun: got %d bytes, but buffer is %d bytes",
              read,
              tup->len);
    }
}

void IM_writeEntry_i32(RM_PageTuple *tup, IM_ENTRY_FORMAT_T *entry, size_t n)
{
    PANIC_IF_NULL(tup);
    PANIC_IF_NULL(entry);
    if (tup->len == 0) { PANIC("length of 'tup' buffer cannot be zero"); }
    if (n == 0) { PANIC("'size' must be greater than zero bytes"); }

    uint16_t wrote = BF_write(
            (BF_MessageElement *) entry,
            &tup->dataBegin,
            BF_NUM_ELEMENTS(n));

    if (wrote == 0) { PANIC("failed to write entry"); }
    if (wrote > tup->len) {
        PANIC("buffer overrun: attempted to write %d bytes, but buffer was %d bytes",
              wrote,
              tup->len);
    }
}

RC IM_readEntryAt_i32(
        BM_BufferPool *pool,
        RID rid,
        IM_ENTRY_FORMAT_T *entry_out)
{
    PANIC_IF_NULL(entry_out);

    BM_PageHandle pageHandle = {};
    TRY_OR_RETURN(pinPage(pool, &pageHandle, rid.page));
    RM_Page *page = (RM_Page *) pageHandle.buffer;

    RM_PageTuple *tup = RM_Page_getTuple(page, rid.slot, NULL);
    IM_readEntry_i32(tup, entry_out, sizeof(IM_ENTRY_FORMAT_T));

    TRY_OR_RETURN(unpinPage(pool, &pageHandle));
    return RC_OK;
}

RC IM_writeEntryAt_i32(
        BM_BufferPool *pool,
        RID rid,
        IM_ENTRY_FORMAT_T *entry_out)
{
    PANIC_IF_NULL(entry_out);

    BM_PageHandle pageHandle = {};
    TRY_OR_RETURN(pinPage(pool, &pageHandle, rid.page));
    RM_Page *page = (RM_Page *) pageHandle.buffer;

    RM_PageTuple *tup = RM_Page_getTuple(page, rid.slot, NULL);
    IM_writeEntry_i32(tup, entry_out, sizeof(IM_ENTRY_FORMAT_T));

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
    BF_SET_U16(entry->idxEntryRidPage) = rid.page;
    BF_SET_U16(entry->idxEntryRidSlot) = rid.slot;
}

RC IM_findIndex(
        BM_BufferPool *pool,
        char *name,
        BM_PageHandle *page_out,
        struct RM_PageTuple **tup_out,
        struct IM_DESCRIPTOR_FORMAT_T *msg_out)
{
    PANIC_IF_NULL(pool);
    PANIC_IF_NULL(name);

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

    if (page_out != NULL) {
        *page_out = pageHandle;
    }

    if (tup_out != NULL) {
        *tup_out = tup;
    }

    if (msg_out != NULL) {
        memcpy(msg_out, &indexMsg, sizeof(IM_DESCRIPTOR_FORMAT));
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
    uint16_t oldPageShift = 0; // use when we pass the insertion point, to offset items
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
            // This is where the node we're inserting will go
            // Increment the page shift by one
            oldPageShift++;

            RM_PageSlotLength len = entryDescriptorSize;
            RM_PageTuple *newTup = RM_Page_reserveTupleAtEnd(leftPage, len);

            // Write out new entry
            uint16_t wrote = BF_write(
                    (BF_MessageElement *) &entry,
                    &newTup->dataBegin,
                    BF_NUM_ELEMENTS(entryDescriptorSize));
            if (wrote > len) { PANIC("buffer overflow"); }
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
            oldPageShift++;

            RM_PageSlotLength len = BF_recomputePhysicalSize(
                    (BF_MessageElement *) entry,
                    BF_NUM_ELEMENTS(entryDescriptorSize));

            RM_PageTuple *newTup = RM_Page_reserveTupleAtEnd(rightPage, len);

            // Write out new entry
            uint16_t wrote = BF_write(
                    (BF_MessageElement *) entry,
                    &newTup->dataBegin,
                    BF_NUM_ELEMENTS(entryDescriptorSize));
            if (wrote > len) { PANIC("buffer overflow"); }
        }
    }

    // Clear all nodes on the old page and reset flags
    RM_page_deleteAllTuples(oldPage);

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
    uint16_t off = isTargetLeft ? 1 : 0;
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
            oldTup = RM_Page_getTuple(oldPage, i + off, NULL);
            len = oldTup->len;
        }

        RM_PageTuple *newTup = RM_Page_reserveTupleAtEnd(rightPage, len);

        if (i == targetSlotIdx) {
            // Copy the insertion entry into the current tuple
            IM_writeEntry_i32(newTup, entry, entryDescriptorSize);

            // Don't forget to increment the offset for the next iteration
            off++;
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

    off = 0; // reset the offset
    if (targetSlotIdx == numLeftFill - 1) {
        // We're inserting the target node in the last position in the left node
        // In this case, we just need to overwrite the last entry
        RM_PageTuple *tup = RM_Page_getTuple(oldPage, targetSlotIdx, NULL);
        IM_writeEntry_i32(tup, entry, sizeof(entryDescriptorSize));
    }
    else if (targetSlotIdx < numLeftFill - 1) {
        // We're inserting the target node before the last element in the
        // new *left* node. We're going to have to shift the slot pointers in
        // the left node over by one to the right.

        // Delete the last element first to prevent an overflow at insert time
        RM_Page_deleteTuple(oldPage, numLeftFill - 1);

        // Insert the tuple, by shifting the rest of the entries over after it
        RM_PageTuple *tup = RM_reserveTupleAtIndex(oldPage, entryPhysSize, targetSlotIdx);
        IM_writeEntry_i32(tup, entry, sizeof(entryDescriptorSize));
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

RC IM_insertKey_i32(
        BM_BufferPool *pool,
        IM_IndexMetadata *indexMeta,
        int32_t keyValue,
        RID rid)
{
    PANIC_IF_NULL(pool);
    PANIC_IF_NULL(indexMeta);

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
    RM_PageNumber parentPageNum;
    RM_PageNumber leafNodePageNum = IM_getLeafNode(
            pool,
            rootPageNum,
            keyValue,
            maxEntriesPerNode,
            &parentPageNum,
            &parents);

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
            targetTup = RM_reserveTupleAtIndex(
                    oldLeafNodePage,
                    entryDiskSize,
                    slotNum);
        }

        IM_writeEntry_i32(targetTup, &entry, sizeof(entry));

        TRY_OR_RETURN(forcePage(pool, &leafNodePageHandle));
        TRY_OR_RETURN(unpinPage(pool, &leafNodePageHandle));

        return RC_OK;
    }

    // We don't have enough space and will need to split the leaf node
    //
    // We will need to turn the current node into a inner node
    // and copy the leaf entries into two separate nodes
    //

    // EDGE CASE: if we're splitting the root node as a leaf node,
    // we need to allocate the new left and right leaf nodes so that we can keep
    // the existing root node as a new inner node
    RC rc;
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

    // Make entry that links to *left* node using
    // the key value of the *right* node's first entry
    RID linkEntryPtrRid = {
            .page = leafSplit.leftPageNum,
            .slot = 0 /* ignored */ };

    IM_ENTRY_FORMAT_T linkEntry;
    IM_makeEntry_i32(&linkEntry, linkEntryKey, linkEntryPtrRid);

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

    BM_PageHandle parentPageHandle = {};
    TRY_OR_RETURN(pinPage(pool, &parentPageHandle, parentPageNum));
    RM_Page *parentPage = (RM_Page *) parentPageHandle.buffer;

    if (!IS_FLAG_SET(parentPage->header.flags, RM_PAGE_FLAGS_INDEX_INNER)) {
        PANIC("expected parent page num (%d) to be marked as an inner node", parentPageNum);
    }

    uint16_t parentNumTuples = parentPage->header.numTuples;
    if (parentNumTuples < maxEntriesPerNode)
    {
        // Parent has enough space to add link to the right leaf node
        uint16_t slotId = IM_getEntryInsertionIndex(linkEntryKey, parentPage, maxEntriesPerNode);
        if (slotId < parentNumTuples) {
            // Insert the link at the current location as a tuple
            RM_PageTuple *linkTup = RM_reserveTupleAtIndex(
                    parentPage,
                    linkEntryPhysSize,
                    slotId);

            // Write out the link tuple
            IM_writeEntry_i32(linkTup, &linkEntry, sizeof(linkEntry));

            // Update the pointer of the next entry to the right node
            uint16_t nextSlotId = slotId + 1;
            RID nextTupRid = {.page = parentPageNum, .slot = nextSlotId};

            IM_ENTRY_FORMAT_T nextEntry;
            IM_readEntryAt_i32(pool, nextTupRid, &nextEntry);

            BF_SET_U16(nextEntry.idxEntryRidPage) = leafSplit.rightPageNum;
            BF_SET_U16(nextEntry.idxEntryRidSlot) = 0;

            IM_writeEntryAt_i32(pool, nextTupRid, &nextEntry);

            return RC_OK;
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
            IM_writeEntry_i32(linkTup, &linkEntry, sizeof(linkEntry));

            // Set the next page num to what we wanted to insert
            // in the first place. If the node was not initialized, then we just
            // set the right-most pointer
            parentPage->header.nextPageNum = leafSplit.rightPageNum;
        }
        else {
            PANIC("'slotId' should never be > 'parentNumTuples'");
        }

        return RC_OK;
    }

    // Parent inner node is full, we're going to need to split it and perform
    // an inner node split
    //
    // (ex)
    //  [1,3,4,5]  ==>   |* 4|*| ~ (inserted into parent)
    //    n = 3          /     \
    //  / / /  \      [1,3]   [5]  (inner nodes)
    //

    NOT_IMPLEMENTED();

    return RC_OK;
}

RM_PageNumber
IM_getLeafNode(
        BM_BufferPool *pool,
        RM_PageNumber rootPageNum,
        int32_t searchKey,
        uint16_t maxEntriesPerNode,
        RM_PageNumber *parent_out,
        IM_NodeTrace **trace_out)
{
    PANIC_IF_NULL(pool);

    BM_PageHandle parentNodeHandle = {};
    RM_Page *parentNode = NULL;

    BM_PageHandle nodeHandle = {};
    RM_Page *node = NULL;

    IM_NodeTrace *trace = NULL;
    if (trace_out != NULL) {
        trace = malloc(sizeof(IM_NodeTrace));
        trace->entryLen = 0;
        trace->entryCapacity = IM_NODETRACE_INITIAL_CAPACITY;
        trace->entryArr = calloc(IM_NODETRACE_INITIAL_CAPACITY, sizeof(*trace->entryArr));
    }

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
            break;
        }

        // Otherwise, the current node must be an internal node, a RID pointer
        // will point to the child node.
        //
        // Find the index that is less than or equal to the search key
        //
        uint16_t slotId = IM_getEntryInsertionIndex(searchKey, node, maxEntriesPerNode);

        // If tracing is enabled, append to the list
        if (trace != NULL) {
            RID nodeRid = {.page = nodePageNum, .slot = slotId};
            IM_NodeTrace_append(trace, nodeRid);
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
            IM_readEntry_i32(nextTup, &entry, sizeof(entry));
            nodePageNum = BF_AS_U16(entry.idxEntryRidPage);
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

    if (unpinPage(pool, &nodeHandle) != RC_OK) {
        PANIC("failed to unpin node page");
    }

    if (parent_out != NULL) {
        if (parentNode != NULL) {
            *parent_out = parentNode->header.pageNum;
            if (unpinPage(pool, &parentNodeHandle) != RC_OK) {
                PANIC("failed to unpin parent node page");
            }
        }
        else {
            *parent_out = rootPageNum;
        }
    }

    if (trace_out != NULL) {
        *trace_out = trace;
    }

    return nodePageNum;
}

uint16_t
IM_getEntryInsertionIndex(
        const int32_t keyValue,
        RM_Page *page,
        uint16_t maxEntriesPerNode)
{
    uint16_t numTuples = page->header.numTuples;

    uint16_t maxSlots =
            (numTuples < maxEntriesPerNode)
            ? numTuples
            : maxEntriesPerNode;

    uint16_t slotNum = 0;
    for (; slotNum < maxSlots; slotNum++) {
        // get position of tuple
        size_t slot = slotNum * sizeof(RM_PageSlotPtr);
        RM_PageTuple *markTup = RM_Page_getTuple(page, slotNum, NULL);

        // read in entry data
        IM_ENTRY_FORMAT_T markEntry;
        IM_readEntry_i32(markTup, &markEntry, sizeof(markEntry));

        // stop if the current key is greater than or equal to the search
        int32_t markKey = BF_AS_I32(markEntry.idxEntryKey);
        if (markKey > keyValue) {
            break;
        }
    }

    return slotNum;
}
