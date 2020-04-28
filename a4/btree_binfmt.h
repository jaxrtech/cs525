#pragma once

#include "dt.h"
#include "binfmt.h"

const struct PACKED_STRUCT IM_DESCRIPTOR_FORMAT_T {
    BF_MessageElement idxName;
    BF_MessageElement idxKeyType;
    BF_MessageElement idxMaxEntriesPerNode;
} IM_DESCRIPTOR_FORMAT = {
        .idxName = {
                .name = "idx_name",
                .type = BF_LSTRING,
        },
        .idxKeyType = {
                .name = "idx_key_type",
                .type = BF_UINT8,
        },
        .idxMaxEntriesPerNode = {
                .name = "idx_max_entries_per_node",
                .type = BF_UINT16,
        },
};

typedef struct PACKED_STRUCT IM_ENTRY_FORMAT_T {
    BF_MessageElement idxEntryKey;
    BF_MessageElement idxEntryRidPage;
    BF_MessageElement idxEntryRidSlot;
} IM_ENTRY_FORMAT_T;

const IM_ENTRY_FORMAT_T IM_ENTRY_FORMAT_OF_INT32 = {
        .idxEntryKey = {
                .name = "idx_entry_key",
                .type = BF_INT32,
        },
        .idxEntryRidPage = {
                .name = "idx_entry_rid_page",
                .type = BF_UINT16,
        },
        .idxEntryRidSlot = {
                .name = "idx_entry_rid_slot",
                .type = BF_UINT16,
        },
};
