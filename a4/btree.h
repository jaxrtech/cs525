#pragma once
#include "rm_page.h"
#include "btree_binfmt.h"
#include "buffer_mgr.h"

#define IM_NODETRACE_INITIAL_CAPACITY (16)

typedef struct IM_IndexMetadata {
    RM_PageNumber rootNodePageNum;
    uint16_t maxEntriesPerNode;
} IM_IndexMetadata;

typedef struct IM_GetNumNodes_Elem {
    RM_PageNumber pageNum;
    RM_PageSlotId nextSlotId;
} IM_GetNumNodes_Elem;

typedef struct IM_NodeTrace {
    RID *entryArr;
    uint16_t entryLen;
    uint16_t entryCapacity;
} IM_NodeTrace;

typedef struct IM_SplitLeafNodeCtx {
    RM_PageNumber leftPageNum;
    RM_PageNumber rightPageNum;
    bool isTargetLeft;
    RM_PageSlotId targetSlot;
} IM_SplitLeafNodeCtx;

typedef uint8_t IM_InsertSplitMode;
#define IM_INSERTSPLITMODE_REUSE_LEFT_ALLOC_RIGHT (1)
#define IM_INSERTSPLITMODE_ALLOC_LEFT_RIGHT (2)


typedef uint8_t IM_GetEntryOp;
#define IM_GETENTRY_OP_COUNT (5)
#define IM_GETENTRY_OP_EQ (0)
#define IM_GETENTRY_OP_LT (1)
#define IM_GETENTRY_OP_GT (2)
#define IM_GETENTRY_OP_GE (3)
#define IM_GETENTRY_OP_LE (4)

typedef int32_t (*IM_GetEntryOpFn)(int32_t, int32_t);
#define IM_GETENTRY_OP_DECL(IDENT, OP) \
    int32_t IM_GETENTRY_OP_##IDENT##_FN(int32_t a, int32_t b) { return a OP b; }

#define IM_GETENTRY_OP_LOOKUP(IDENT) \
    [IM_GETENTRY_OP_##IDENT] = IM_GETENTRY_OP_##IDENT##_FN

RID IM_makeRidFromEntry(
        IM_ENTRY_FORMAT_T *entry);

void
IM_IndexMetadata_makeFromMessage(
        IM_IndexMetadata *meta,
        IM_DESCRIPTOR_FORMAT_T *indexMsg);

uint16_t
IM_getEntryInsertionIndex(
        int32_t keyValue,
        RM_Page *page,
        uint16_t maxEntriesPerNode);

RM_PageNumber
IM_getLeafNode(
        BM_BufferPool *pool,
        RM_PageNumber rootPageNum,
        int32_t searchKey,
        uint16_t maxEntriesPerNode,
        RID *parent_out_opt,
        IM_NodeTrace **trace_out_opt);

RC
IM_writeIndexPage(BM_BufferPool *pool);

RC
IM_insertKey_i32(
        BM_BufferPool *pool,
        IM_IndexMetadata *indexMeta,
        int32_t keyValue,
        RID rid);

RC
IM_findIndex(
        BM_BufferPool *pool,
        char *name,
        BM_PageHandle *descriptor_page_out_opt,
        RM_PageTuple **tup_out_opt,
        struct IM_DESCRIPTOR_FORMAT_T *msg_out_opt);

bool
IM_getEntryIndex(
        int32_t keyValue,
        RM_Page *page,
        uint16_t maxEntriesPerNode,
        IM_ENTRY_FORMAT_T *entry_out_opt,
        uint16_t *slotId_out_opt);

void
IM_readEntry_i32(RM_PageTuple *tup, IM_ENTRY_FORMAT_T *result);

RC
IM_readEntryAt_i32(
        BM_BufferPool *pool,
        RID rid,
        IM_ENTRY_FORMAT_T *entry_out);

RC
IM_findEntry_i32(
        BM_BufferPool *pool,
        IM_IndexMetadata *indexMeta,
        int32_t keyValue,
        RID *entryValue_out_opt,
        RID *entryIndex_out_opt,
        RID *parentLinkRid_out_opt);

RC
IM_writeEntryAt_i32(
        BM_BufferPool *pool,
        RID rid,
        IM_ENTRY_FORMAT_T *entry_out);

RC
IM_deleteIndex(
        BM_BufferPool *pool,
        char *idxId);

RC
IM_getNumNodes(
        BM_BufferPool *pool,
        IM_IndexMetadata *indexMeta,
        int *result_out);

RC
IM_deleteKey_i32(
        BM_BufferPool *pool,
        IM_IndexMetadata *indexMeta,
        int32_t keyValue);

RC IM_readEntryValueAt(
        BM_BufferPool *pool,
        RID entryIndex,
        RID *entryValue_out);

uint16_t
IM_getEntryIndexByPredicate(
        int32_t keyValue,
        RM_Page *page,
        IM_GetEntryOp op,
        uint16_t maxEntriesPerNode,
        IM_ENTRY_FORMAT_T *entry_out_opt,
        uint16_t *numSlots_out_opt);