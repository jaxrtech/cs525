#pragma once

#include <stdint.h>
#include <assert.h>

#include "tables.h"

#define RM_DATABASE_MAGIC "FANCYDB"
#define RM_DATABASE_MAGIC_LEN  (8)
//static_assert(sizeof(RM_DATABASE_MAGIC) <= RM_DATABASE_MAGIC_LEN, "database magic length mismatch");

#define PACKED_STRUCT __attribute__((__packed__))

typedef uint16_t RM_PageNumber;

typedef struct PACKED_STRUCT RM_DatabaseHeader {
    char magic[RM_DATABASE_MAGIC_LEN];
    uint16_t pageSize;
    RM_PageNumber numPages;
    RM_PageNumber schemaPageNum;
} RM_DatabaseHeader;

typedef uint16_t RM_PageFlags;
#define RM_PAGE_FLAGS_HAS_FREE_PTRS  ((RM_PageFlags) (1u << 0u))  /* if page has space for additional slot pointers */
#define RM_PAGE_FLAGS_TUPS_FULL      ((RM_PageFlags) (1u << 1u))  /* if page has no space for additional tuples */
#define RM_PAGE_FLAGS_HAS_TRAILING   ((RM_PageFlags) (1u << 2u))  /* if page has trailing re-space after tuples */
#define RM_PAGE_FLAGS_INDEX_ROOT     ((RM_PageFlags) (1u << 3u))
#define RM_PAGE_FLAGS_INDEX_INNER    ((RM_PageFlags) (1u << 4u))
#define RM_PAGE_FLAGS_INDEX_LEAF     ((RM_PageFlags) (1u << 5u))

typedef uint8_t RM_PageKind;
#define RM_PAGE_KIND_SCHEMA 1
#define RM_PAGE_KIND_DATA   2
#define RM_PAGE_KIND_INDEX  3
#define RM_PAGE_KIND_FREE   0xff

typedef struct PACKED_STRUCT RM_PageHeader {
    RM_PageNumber pageNum;
    RM_PageKind kind;
    RM_PageFlags flags;
    uint16_t numTuples;

    /**
     * The **inclusive** byte offset into the page where the next available "left-most" byte would be allocated.
     * The shall never be a valid slot pointer at or after this offset.
     */
    uint16_t freespaceLowerOffset;

    /**
     * The **exclusive** byte offset into the page for the available "right-most" byte would be allocated.
     * There shall only be tuples after this offset or be past the end of the page at this offset.
     */
    uint16_t freespaceUpperEnd;

    /**
     * The byte offset into the page where the next free byte would be allocated after the last valid tuple.
     * This would only be set if a tuple at the end of the page was removed from the page.
     * The flag `RM_PAGE_FLAGS_HAS_TRAILING` must be set for this value to be taken into account.
     */
    uint16_t freespaceTrailingOffset;

    /**
     * recordID for schema?
     */
    uint16_t RID;

    /**
     * integer in case data in table exceeds 4096 Bytes (set to -1 if only one page)
     * allows the pages of a table to coalesce in a linkedlist fashion
     */
    int32_t nextPageNum;

} RM_PageHeader;

#define RM_PAGE_NEXT_PAGENUM_UNSET   ((uint16_t) -1)
#define RM_PAGE_NEXT_PAGENUM_INVALID ((int32_t) INT32_MIN)
#define RM_PAGE_DATA_SIZE (PAGE_SIZE - sizeof(RM_PageHeader))

typedef struct PACKED_STRUCT RM_Page {
    RM_PageHeader header;

    /**
     * Used as a marker for the beginning of the data buffer.
     * Should always be used as `&page->dataBegin`.
     */
    char dataBegin;
} RM_Page;

typedef uint16_t RM_PageSlotPtr;
typedef uint16_t RM_PageSlotId;
typedef uint16_t RM_PageSlotLength;

typedef struct PACKED_STRUCT RM_PageTuple {
    RM_PageSlotId slotId;
    RM_PageSlotLength len;

    /**
     * Used as a marker for the beginning of the data buffer.
     * Should always be used as `&page->dataBegin`.
     */
    char dataBegin;
} RM_PageTuple;

#define RM_TUP_SIZE(DATA_SIZE) \
    sizeof(RM_PageSlotId) + sizeof(RM_PageSlotLength) + (DATA_SIZE)

RM_Page *RM_Page_init(void *buffer, RM_PageNumber pageNumber, RM_PageKind kind);
RM_PageTuple *RM_Page_reserveTupleAtEnd(RM_Page *self, uint16_t len);
RM_PageTuple *RM_reserveTupleAtIndex(RM_Page *page, uint16_t slotNum, const uint16_t len);

RM_PageTuple *RM_Page_getTuple(
        RM_Page *self,
        RM_PageSlotId slotIdx,
        RM_PageSlotPtr **ptr_out);
void RM_Page_getRecord(RM_Page *self, Record *record, RID rid);
void RM_Page_setTuple(RM_Page *self, Record *r);
void RM_Page_deleteTuple(RM_Page *self, RM_PageSlotId slotId);
void RM_page_deleteAllTuples(RM_Page *self);
