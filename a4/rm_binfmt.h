#pragma once

#include "dt.h"
#include "binfmt.h"

const struct PACKED_STRUCT RM_SCHEMA_ATTR_FORMAT_T {
    BF_MessageElement attrType;
    BF_MessageElement attrTypeLen;
    BF_MessageElement attrName;
} RM_SCHEMA_ATTR_FORMAT = {
        .attrType = {
                .name = "attr_type",
                .type = BF_UINT8,
        },
        .attrTypeLen = {
                .name = "attr_type_len",
                .type = BF_UINT8,
        },
        .attrName = {
                .name = "attr_name",
                .type = BF_LSTRING,
        },
};

const struct PACKED_STRUCT RM_SCHEMA_FORMAT_T {
    BF_MessageElement tblName;
    BF_MessageElement tblDataPageNum;
    BF_MessageElement tblKeys;
    BF_MessageElement tblNumAttr;
    BF_MessageElement tblAttrs;
} RM_SCHEMA_FORMAT = {
        .tblName = {
                .name = "tbl_name",
                .type = BF_LSTRING,
        },
        .tblDataPageNum = {
                .name = "tbl_data_pgnum",
                .type = BF_UINT16,
        },
        .tblNumAttr = {
                .name = "tbl_num_attr",
                .type = BF_UINT8,
        },
        .tblKeys = {
                .name = "tbl_keys",
                .type = BF_ARRAY_UINT8,
        },
        .tblAttrs = {
                .name = "tbl_attrs",
                .type = BF_ARRAY_MSG,
                .array_msg = {
                        .type_count = sizeof(RM_SCHEMA_ATTR_FORMAT) / sizeof(BF_MessageElement),
                        .type = (const struct BF_MessageElement *) &RM_SCHEMA_ATTR_FORMAT,
                },
        }
};
