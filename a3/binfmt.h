#pragma once

#include <stdbool.h>
#include <stdint.h>
#include "dt.h"

typedef enum BF_DataType {
    BF_UINT8,
    BF_LSTRING,
    BF_ARRAY_UINT8,
    BF_ARRAY_LMSG,
} BF_DataType;

struct BF_MessageElement;

typedef struct BF_MessageElement {
    char* name;
    BF_DataType type;
    uint16_t cached_size;
    union {
        uint8_t u8;
        struct {
            uint8_t cached_strlen;
            char *str;
        } lstring;
        struct {
            uint8_t len;
            uint8_t *buf;
        } array_u8;
        struct {
            uint8_t type_count;
            const struct BF_MessageElement *type;
            uint8_t data_count;
            struct BF_MessageElement *data;
        } array_msg;
    };
} BF_MessageElement;

#define RM_BUF_READ(BUFFER, TYPE, DEST) \
    do { \
        DEST = *(TYPE *)(BUFFER); \
        BUFFER = ((char *) (BUFFER)) + sizeof(TYPE); \
    } while (0)

//write val to buffer by casting val to TYPE pointer and dereferencing it
#define RM_BUF_WRITE(BUFFER, TYPE, VAL) \
    do { \
        *((TYPE *) (BUFFER)) = (TYPE)(VAL); \
        BUFFER = ((char *) (BUFFER)) + sizeof(TYPE); \
    } while (0)

#define RM_BUF_WRITE_FROM(BUFFER, PTR, LEN) \
    do { \
        memcpy((BUFFER), (PTR), (LEN)); \
        BUFFER = ((char *) (BUFFER)) + (LEN); \
    } while (0)

//writes the length of the string  and then writes the string (up to 255 chars)
#define RM_BUF_WRITE_LSTRING(BUFFER, STR_PTR, LEN) \
    do { \
        RM_BUF_WRITE((BUFFER), uint8_t, (LEN)); \
        RM_BUF_WRITE_FROM((BUFFER), (STR_PTR), (LEN)); \
    } while (0)


uint16_t
BF_recomputeSize(BF_MessageElement *self);

uint16_t
BF_recomputeSize_arr(BF_MessageElement *arr, uint8_t num_elements);

uint16_t
BF_write(BF_MessageElement *self, void *buffer);

uint16_t
BF_write_arr(BF_MessageElement *arr, void *buffer, uint8_t num_elements);

uint16_t
BF_read(BF_MessageElement *self, void *buffer);

uint16_t
BF_read_arr(BF_MessageElement *arr, void *buffer, uint8_t num_elements);