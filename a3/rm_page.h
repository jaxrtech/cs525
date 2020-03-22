#pragma once

#include <stdint.h>
#include <assert.h>

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
#define RM_PAGE_FLAGS_HAS_FREE_PTRS  (1u << 0u)  /* if page has space for additional slot pointers */
#define RM_PAGE_FLAGS_TUPS_FULL      (1u << 1u)  /* if page has no space for additional tuples */
#define RM_PAGE_FLAGS_HAS_TRAILING   (1u << 2u)  /* if page has trailing re-space after tuples */

typedef uint8_t RM_PageKind;
#define RM_PAGE_KIND_SCHEMA 1
#define RM_PAGE_KIND_DATA   2

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
    int nextPageNum;


} RM_PageHeader;

#define RM_PAGE_DATA_SIZE (PAGE_SIZE - sizeof(RM_PageHeader))

typedef struct PACKED_STRUCT RM_Page {
    RM_PageHeader header;
    char *data; //series of pointers
} RM_Page;

typedef uint16_t RM_PageSlotPtr;
typedef uint16_t RM_PageSlotId;
typedef uint16_t RM_PageSlotLength;

typedef struct PACKED_STRUCT RM_PageTuple {
    RM_PageSlotId slotId;
    RM_PageSlotLength len;
    void *data;
} RM_PageTuple;

RM_Page *RM_Page_init(void *buffer, RM_PageNumber pageNumber, RM_PageKind kind);
void *RM_Page_reserveTuple(RM_Page *self, uint16_t len);
