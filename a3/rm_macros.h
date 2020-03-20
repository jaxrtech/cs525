#pragma once

#include <stdio.h>
#include <stdlib.h>

#define NOT_IMPLEMENTED() \
    do { \
        fprintf(stderr, "NOT IMPLEMENTED: [%s:%d] function %s\n", __FILE__, __LINE__, __FUNCTION__); \
        exit(1); \
    } while (0)

#define PANIC(msg, vargs...) \
    do { \
        fprintf(stderr, "panic! [%s] %s:L%d: " msg "\n", \
                __FILE__,  \
                __FUNCTION__, \
                __LINE__, \
                ##vargs); \
        fflush(stderr); \
        exit(1); \
    } while (0)

#define TRY_OR_RETURN(ACTION) \
    do { \
        RC __rc; \
        if ((__rc = (ACTION)) != RC_OK) { return __rc; } \
    } while (0)

#define IGNORE_UNUSED __attribute__((unused))

#define IS_FLAG_SET(FLAG, MASK)    (((FLAG) & (MASK)) != 0)
#define IS_FLAG_UNSET(FLAG, MASK)  (((FLAG) & (MASK)) == 0)

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

#define RM_BUF_WRITE_LSTRING(BUFFER, STR_PTR, LEN) \
    do { \
        RM_BUF_WRITE((BUFFER), uint8_t, (LEN)); \
        RM_BUF_WRITE_FROM((BUFFER), (STR_PTR), (LEN)); \
    } while (0)
