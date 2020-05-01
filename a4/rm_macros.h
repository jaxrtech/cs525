#pragma once

#include <stdio.h>
#include <stdlib.h>
#include <stdarg.h>

#ifndef __RM_PANIC_FMT__
#define __RM_PANIC_FMT__
__attribute__((noreturn)) static void *
panic_fmt(char *fmt, ...)
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

#define IS_FLAG_SET(X, FLAG)    (((X) & (FLAG)) != 0)
#define IS_FLAG_UNSET(X, FLAG)  (((X) & (FLAG)) == 0)

#define SET_FLAG(X, FLAG) \
    do { \
        X |= FLAG; \
    } while (0)

#define UNSET_FLAG(X, FLAG) \
    do { \
        X &= (typeof(X)) ~(FLAG); \
    } while (0)
