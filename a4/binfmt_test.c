#include <stdio.h>
#include <alloca.h>
#include <string.h>
#include "binfmt.h"
#include "rm_binfmt.h"
#include "tables.h"

const struct PACKED_STRUCT TEST_SINGLE_T {
    BF_MessageElement a;
} TEST_SINGLE = {
        .a = {
                .name = "test_a",
                .type = BF_UINT8,
        },
};

const struct PACKED_STRUCT TEST_LSTRING_T {
    BF_MessageElement str;
} TEST_LSTRING = {
        .str = {
                .name = "test_str",
                .type = BF_LSTRING,
        },
};

const struct PACKED_STRUCT TEST_ARRAY_UINT8_T {
    BF_MessageElement xs;
} TEST_ARRAY_UINT8 = {
        .xs = {
                .name = "test_u8_arr",
                .type = BF_ARRAY_UINT8,
        },
};

const struct PACKED_STRUCT TEST_MULTI_T {
    BF_MessageElement a;
    BF_MessageElement b;
    BF_MessageElement c;
} TEST_MULTI = {
        .a = {
                .name = "test_a",
                .type = BF_UINT8
        },
        .b = {
                .name = "test_b",
                .type = BF_UINT8
        },
        .c = {
                .name = "test_c",
                .type = BF_UINT8
        },
};

const struct PACKED_STRUCT TEST_NESTED_T {
    BF_MessageElement nested;
} TEST_NESTED = {
        .nested = {
                .name = "test_nested",
                .type = BF_ARRAY_MSG,
                .array_msg = {
                        .type_count = sizeof(struct TEST_MULTI_T) / sizeof(BF_MessageElement),
                        .type = (BF_MessageElement *) &TEST_MULTI,
                },
        }
};

bool expect_uint8_to_round_trip()
{
    struct TEST_SINGLE_T msg = TEST_SINGLE;
    msg.a.u8 = 42;

    uint16_t size = BF_recomputeSize_single((BF_MessageElement *) &msg);

    void *buffer = alloca(size);
    uint16_t write_size = BF_write_single((BF_MessageElement *) &msg, buffer);
    if (write_size != size) {
        fprintf(stderr, "[%s:%d] FAIL: expected write size to be the same recomputed size\n", __FUNCTION__, __LINE__);
        return false;
    }

    struct TEST_SINGLE_T msg_read = TEST_SINGLE;
    uint16_t read_size = BF_read_single((BF_MessageElement *) &msg_read, buffer);
    if (read_size != size) {
        fprintf(stderr, "[%s:%d] FAIL: expected read size to be the same recomputed size\n", __FUNCTION__, __LINE__);
        return false;
    }

    if (msg_read.a.u8 != msg.a.u8) {
        fprintf(stderr, "[%s:%d] FAIL: expected value to be the same\n", __FUNCTION__, __LINE__);
        return false;
    }

    return true;
}

bool expect_uint8_multi_to_round_trip()
{
    struct TEST_MULTI_T msg = TEST_MULTI;
    msg.a.u8 = 11;
    msg.b.u8 = 22;
    msg.c.u8 = 33;

    const size_t num_elements = sizeof(struct TEST_MULTI_T) / sizeof(BF_MessageElement);
    uint16_t size = BF_recomputeSize((BF_MessageElement *) &msg, num_elements);

    void *buffer = alloca(size);
    uint16_t write_size = BF_write((BF_MessageElement *) &msg, buffer, num_elements);
    if (write_size != size) {
        fprintf(stderr, "[%s:%d] FAIL: expected write size to be the same recomputed size\n", __FUNCTION__, __LINE__);
        return false;
    }

    struct TEST_MULTI_T msg_read = TEST_MULTI;
    uint16_t read_size = BF_read((BF_MessageElement *) &msg_read, buffer, num_elements);
    if (read_size != size) {
        fprintf(stderr, "[%s:%d] FAIL: expected read size to be the same recomputed size\n", __FUNCTION__, __LINE__);
        return false;
    }

    if (msg_read.a.u8 != msg.a.u8) {
        fprintf(stderr, "[%s:%d] FAIL: expected value to be the same\n", __FUNCTION__, __LINE__);
        return false;
    }

    if (msg_read.b.u8 != msg.b.u8) {
        fprintf(stderr, "[%s:%d] FAIL: expected value to be the same\n", __FUNCTION__, __LINE__);
        return false;
    }

    if (msg_read.c.u8 != msg.c.u8) {
        fprintf(stderr, "[%s:%d] FAIL: expected value to be the same\n", __FUNCTION__, __LINE__);
        return false;
    }

    return true;
}

bool expect_lstring_to_round_trip()
{
    static char test_str[] = "hello world";
    struct TEST_LSTRING_T msg = TEST_LSTRING;
    msg.str.lstring.str = test_str;

    uint16_t size = BF_recomputeSize_single((BF_MessageElement *) &msg);
    if (msg.str.lstring.cached_strlen != sizeof(test_str)) {
        fprintf(stderr, "[%s:%d] FAIL: expected the cached length to be the same as the test string length\n", __FUNCTION__, __LINE__);
        return false;
    }

    void *buffer = alloca(size);
    uint16_t write_size = BF_write_single((BF_MessageElement *) &msg, buffer);
    if (write_size != size) {
        fprintf(stderr, "[%s:%d] FAIL: expected write size to be the same recomputed size\n", __FUNCTION__, __LINE__);
        return false;
    }

    struct TEST_LSTRING_T msg_read = TEST_LSTRING;
    uint16_t read_size = BF_read_single((BF_MessageElement *) &msg_read, buffer);
    if (read_size != size) {
        fprintf(stderr, "[%s:%d] FAIL: expected write size to be the same recomputed size\n", __FUNCTION__, __LINE__);
        return false;
    }

    uint8_t exptected_strlen = msg.str.lstring.cached_strlen;
    uint8_t read_strlen = msg_read.str.lstring.cached_strlen;
    if (exptected_strlen != read_strlen) {
        fprintf(stderr, "[%s:%d] FAIL: expected length of string buffer to be the same\n", __FUNCTION__, __LINE__);
        return false;
    }

    if (strncmp(msg.str.lstring.str, msg_read.str.lstring.str, msg_read.str.lstring.cached_strlen) != 0) {
        fprintf(stderr, "[%s:%d] FAIL: expected string value to be the same\n", __FUNCTION__, __LINE__);
        return false;
    }

    return true;
}

bool expect_arr_u8_to_round_trip()
{
    static uint8_t test_arr[] = {1,2,3,4,5,6,7,8,9};
    struct TEST_ARRAY_UINT8_T msg = TEST_ARRAY_UINT8;
    msg.xs.array_u8.buf = test_arr;
    msg.xs.array_u8.len = sizeof(test_arr);

    uint16_t size = BF_recomputeSize_single((BF_MessageElement *) &msg);
    if (msg.xs.array_u8.len != sizeof(test_arr)) {
        fprintf(stderr, "[%s:%d] FAIL: expected the cached length to be the same as the test array length\n", __FUNCTION__, __LINE__);
        return false;
    }

    if (size != sizeof(test_arr) + sizeof(uint8_t) /* len prefix */) {
        fprintf(stderr, "[%s:%d] FAIL: expected the message size to be size of array + 1 byte for length\n", __FUNCTION__, __LINE__);
        return false;
    }

    void *buffer = alloca(size);
    uint16_t write_size = BF_write_single((BF_MessageElement *) &msg, buffer);
    if (write_size != size) {
        fprintf(stderr, "[%s:%d] FAIL: expected write size to be the same recomputed size\n", __FUNCTION__, __LINE__);
        return false;
    }

    struct TEST_ARRAY_UINT8_T msg_read = TEST_ARRAY_UINT8;
    uint16_t read_size = BF_read_single((BF_MessageElement *) &msg_read, buffer);
    if (read_size != size) {
        fprintf(stderr, "[%s:%d] FAIL: expected write size to be the same recomputed size\n", __FUNCTION__, __LINE__);
        return false;
    }

    uint8_t exptected_strlen = msg.xs.array_u8.len;
    uint8_t read_strlen = msg_read.xs.array_u8.len;
    if (exptected_strlen != read_strlen) {
        fprintf(stderr, "[%s:%d] FAIL: expected length of array buffer to be the same\n", __FUNCTION__, __LINE__);
        return false;
    }

    if (memcmp(msg.xs.array_u8.buf, msg_read.xs.array_u8.buf, msg.xs.array_u8.len) != 0) {
        fprintf(stderr, "[%s:%d] FAIL: expected string value to be the same\n", __FUNCTION__, __LINE__);
        return false;
    }

    return true;
}

bool expect_nested_lmsg_to_round_trip()
{
#define NUM_CHUNKS (3)
    struct TEST_MULTI_T chunks[NUM_CHUNKS] = {};
    chunks[0] = TEST_MULTI;
    chunks[0].a.u8 = 11;
    chunks[0].b.u8 = 22;
    chunks[0].c.u8 = 33;

    chunks[1] = TEST_MULTI;
    chunks[1].a.u8 = 44;
    chunks[1].b.u8 = 55;
    chunks[1].c.u8 = 66;

    chunks[2] = TEST_MULTI;
    chunks[2].a.u8 = 77;
    chunks[2].b.u8 = 88;
    chunks[2].c.u8 = 99;

    struct TEST_NESTED_T msg = TEST_NESTED;
    msg.nested.array_msg.data_count = sizeof(struct TEST_MULTI_T) / sizeof(BF_MessageElement) * 3;
    msg.nested.array_msg.data = (BF_MessageElement *) &chunks;

    uint16_t size = BF_recomputeSize_single((BF_MessageElement *) &msg);

    void *buffer = alloca(size);
    uint16_t write_size = BF_write_single((BF_MessageElement *) &msg, buffer);
    if (write_size != size) {
        fprintf(stderr, "[%s:%d] FAIL: expected write size to be the same recomputed size\n", __FUNCTION__, __LINE__);
        return false;
    }

    struct TEST_NESTED_T msg_read = TEST_NESTED;
    uint16_t read_size = BF_read_single((BF_MessageElement *) &msg_read, buffer);
    if (read_size != size) {
        fprintf(stderr, "[%s:%d] FAIL: expected write size to be the same recomputed size\n", __FUNCTION__, __LINE__);
        return false;
    }

    struct TEST_MULTI_T cur_chunk = TEST_MULTI;
    struct TEST_MULTI_T *ptr = (struct TEST_MULTI_T *) msg_read.nested.array_msg.data;
    for (int i = 0; i < NUM_CHUNKS; i++) {
        cur_chunk = ptr[i];
        if (cur_chunk.a.u8 != chunks[i].a.u8) {
            fprintf(stderr, "[%s:%d] FAIL: expected value to be the same\n", __FUNCTION__, __LINE__);
            return false;
        }

        if (cur_chunk.b.u8 != chunks[i].b.u8) {
            fprintf(stderr, "[%s:%d] FAIL: expected value to be the same\n", __FUNCTION__, __LINE__);
            return false;
        }

        if (cur_chunk.c.u8 != chunks[i].c.u8) {
            fprintf(stderr, "[%s:%d] FAIL: expected value to be the same\n", __FUNCTION__, __LINE__);
            return false;
        }
    }

    return true;
#undef NUM_CHUNKS
}

bool expect_nested_schema_format_to_round_trip()
{
#define NUM_COLUMNS (4)
    struct RM_SCHEMA_ATTR_FORMAT_T attrs[NUM_COLUMNS] = {};
    attrs[0] = RM_SCHEMA_ATTR_FORMAT;
    BF_SET_U8 (attrs[0].attrType) = (uint8_t) DT_INT;
    BF_SET_STR(attrs[0].attrName) = "id";
    BF_SET_U8 (attrs[0].attrTypeLen) = 0;

    attrs[1] = RM_SCHEMA_ATTR_FORMAT;
    BF_SET_U8 (attrs[1].attrType) = (uint8_t) DT_STRING;
    BF_SET_STR(attrs[1].attrName) = "name";
    BF_SET_U8 (attrs[1].attrTypeLen) = 200;

    attrs[2] = RM_SCHEMA_ATTR_FORMAT;
    BF_SET_U8 (attrs[2].attrType) = (uint8_t) DT_FLOAT;
    BF_SET_STR(attrs[2].attrName) = "amount";
    BF_SET_U8 (attrs[2].attrTypeLen) = 0;

    attrs[3] = RM_SCHEMA_ATTR_FORMAT;
    BF_SET_U8 (attrs[3].attrType) = (uint8_t) DT_BOOL;
    BF_SET_STR(attrs[3].attrName) = "is_employee";
    BF_SET_U8 (attrs[3].attrTypeLen) = 0;

    struct RM_SCHEMA_FORMAT_T schema = RM_SCHEMA_FORMAT;
    BF_SET_STR(schema.tblName) = "test_table";
    BF_SET_U8 (schema.tblNumAttr) = NUM_COLUMNS;
    uint8_t schema_keys[1] = {0}; // "id" column
    BF_SET_ARRAY_U8(schema.tblKeys, schema_keys, 1);
    BF_SET_ARRAY_MSG(schema.tblAttrs, attrs, sizeof(attrs));

    size_t num_elements = sizeof(schema) / sizeof(BF_MessageElement);
    uint16_t size = BF_recomputeSize((BF_MessageElement *) &schema, num_elements);

    void *buffer = alloca(size);
    uint16_t write_size = BF_write((BF_MessageElement *) &schema, buffer, num_elements);
    if (write_size != size) {
        fprintf(stderr, "[%s:%d] FAIL: expected write size to be the same recomputed size\n", __FUNCTION__, __LINE__);
        return false;
    }

    struct RM_SCHEMA_FORMAT_T msg_read = RM_SCHEMA_FORMAT;
    uint16_t read_size = BF_read((BF_MessageElement *) &msg_read, buffer, num_elements);
    if (read_size != size) {
        fprintf(stderr, "[%s:%d] FAIL: expected read size to be the same recomputed size\n", __FUNCTION__, __LINE__);
        return false;
    }

    if (strncmp(BF_AS_STR(msg_read.tblName), BF_AS_STR(schema.tblName), BF_STRLEN(schema.tblName)) != 0) {
        fprintf(stderr, "[%s:%d] FAIL: expected value to be the same\n", __FUNCTION__, __LINE__);
        return false;
    }

    if (BF_AS_U8(msg_read.tblNumAttr) != BF_AS_U8(schema.tblNumAttr)) {
        fprintf(stderr, "[%s:%d] FAIL: expected value to be the same\n", __FUNCTION__, __LINE__);
        return false;
    }

    if (memcmp(BF_AS_ARRAY_U8(msg_read.tblKeys), BF_AS_ARRAY_U8(schema.tblKeys), BF_ARRAY_U8_LEN(schema.tblKeys)) != 0) {
        fprintf(stderr, "[%s:%d] FAIL: expected value to be the same\n", __FUNCTION__, __LINE__);
        return false;
    }

    struct RM_SCHEMA_ATTR_FORMAT_T *cur_chunk;
    struct RM_SCHEMA_ATTR_FORMAT_T *ptr = (struct RM_SCHEMA_ATTR_FORMAT_T *) msg_read.tblAttrs.array_msg.data;
    for (int i = 0; i < NUM_COLUMNS; i++) {
        cur_chunk = &ptr[i];
        if (strncmp(BF_AS_STR(cur_chunk->attrName), BF_AS_STR(attrs[i].attrName), BF_STRLEN(attrs[i].attrName)) != 0) {
            fprintf(stderr, "[%s:%d] FAIL: expected value to be the same\n", __FUNCTION__, __LINE__);
            return false;
        }

        if (cur_chunk->attrType.u8 != attrs[i].attrType.u8) {
            fprintf(stderr, "[%s:%d] FAIL: expected value to be the same\n", __FUNCTION__, __LINE__);
            return false;
        }

        if (cur_chunk->attrTypeLen.u8 != attrs[i].attrTypeLen.u8) {
            fprintf(stderr, "[%s:%d] FAIL: expected value to be the same\n", __FUNCTION__, __LINE__);
            return false;
        }
    }

    return true;
#undef NUM_COLUMNS
}


int main()
{
    printf("sizeof(BF_MessageElement) = %lu\n", sizeof(BF_MessageElement));
    printf("sizeof(BF_SCHEMA_ATTR_FORMAT_T) = %lu\n", sizeof(struct RM_SCHEMA_ATTR_FORMAT_T));
    printf("sizeof(BF_SCHEMA_FORMAT_T) = %lu\n", sizeof(struct RM_SCHEMA_FORMAT_T));

    bool did_succeed = true;
    did_succeed = did_succeed && expect_uint8_to_round_trip();
    did_succeed = did_succeed && expect_lstring_to_round_trip();
    did_succeed = did_succeed && expect_uint8_multi_to_round_trip();
    did_succeed = did_succeed && expect_arr_u8_to_round_trip();
    did_succeed = did_succeed && expect_nested_lmsg_to_round_trip();
    did_succeed = did_succeed && expect_nested_schema_format_to_round_trip();

    if (did_succeed) {
        printf("PASSED\n");
    }
}