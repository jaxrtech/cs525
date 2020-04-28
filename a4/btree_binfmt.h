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
