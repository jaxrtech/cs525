#pragma once

#include <stdio.h>
#include <stdlib.h>
#include <stdarg.h>

#ifndef __RM_PANIC_FMT__
#define __RM_PANIC_FMT__
static void *panic_fmt(char *fmt, ...)
{
    va_list myargs;
    va_start(myargs, fmt);

    vfprintf(stderr, fmt, myargs);
    fflush(stderr);
    va_end(myargs);

    exit(1);
}
#endif

#define MARKER(n) \
    do {fprintf(stderr, "MARKER %d: [%s:%d] function %s\n", n, __FILE__, __LINE__, __FUNCTION__); } while(0)

#define NOT_IMPLEMENTED() \
    do { \
        fprintf(stderr, "NOT IMPLEMENTED: [%s:%d] function %s\n", __FILE__, __LINE__, __FUNCTION__); \
        exit(1); \
    } while (0)

#define PANIC(msg, vargs...) \
    panic_fmt("panic! [%s:%d] %s: " msg "\n", \
            __FILE__,  \
            __LINE__, \
            __FUNCTION__, \
            ##vargs)

//tries to do ACTION and returns the RC if it fails
#define TRY_OR_RETURN(ACTION) \
    do { \
        RC __rc; \
        if ((__rc = (ACTION)) != RC_OK) { return __rc; } \
    } while (0)

#define IGNORE_UNUSED __attribute__((unused))

#define IS_FLAG_SET(FLAG, MASK)    (((FLAG) & (MASK)) != 0)
#define IS_FLAG_UNSET(FLAG, MASK)  (((FLAG) & (MASK)) == 0)
