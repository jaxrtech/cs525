#pragma once

#include "dt.h"
#include "binfmt.h"

typedef struct PACKED_STRUCT IM_DESCRIPTOR_FORMAT_T {
    BF_MessageElement idxName;
    BF_MessageElement idxKeyType;
    BF_MessageElement idxMaxEntriesPerNode;
    BF_MessageElement idxRootNodePageNum;
} IM_DESCRIPTOR_FORMAT_T;

typedef struct PACKED_STRUCT IM_ENTRY_FORMAT_T {
    BF_MessageElement idxEntryKey;
    BF_MessageElement idxEntryRidPageNum;
    BF_MessageElement idxEntryRidSlot;
} IM_ENTRY_FORMAT_T;

extern const IM_DESCRIPTOR_FORMAT_T IM_DESCRIPTOR_FORMAT;
extern const IM_ENTRY_FORMAT_T IM_ENTRY_FORMAT_OF_I32;
