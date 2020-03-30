#pragma once

#include "dt.h"
#include "binfmt.h"

const struct PACKED_STRUCT BF_SCHEMA_ATTR_FORMAT_T {
    BF_MessageElement attrType;
    BF_MessageElement attrTypeLenOpt;
    BF_MessageElement attrName;
} BF_SCHEMA_ATTR_FORMAT = {
        .attrType = {
                .name = "attr_type",
                .type = BF_UINT8,
        },
        .attrTypeLenOpt = {
                .name = "attr_type_len",
                .type = BF_UINT8,
        },
        .attrName = {
                .name = "attr_name",
                .type = BF_LSTRING,
        },
};

const struct PACKED_STRUCT BF_SCHEMA_FORMAT_T {
    BF_MessageElement tblName;
    BF_MessageElement tblNumAttr;
    BF_MessageElement tblKeys;
    BF_MessageElement tblAttrs;
} BF_SCHEMA_FORMAT = {
        .tblName = {
                .name = "tbl_name",
                .type = BF_LSTRING,
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
                .type = BF_ARRAY_LMSG,
                .array_msg = {
                        .type_count = sizeof(BF_SCHEMA_ATTR_FORMAT) / sizeof(BF_MessageElement),
                        .type = (const struct BF_MessageElement *) &BF_SCHEMA_ATTR_FORMAT,
                },
        }
};
