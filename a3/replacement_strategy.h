#ifndef __REPLACEMENT_STRATEGY_H__
#define __REPLACEMENT_STRATEGY_H__

#include "linked_list.h"
#include "buffer_mgr.h"

typedef struct RS_StrategyHandler {
    ReplacementStrategy strategy;

    void (*init)(BM_BufferPool *pool);

    void (*free)(BM_BufferPool *poll);

    void (*insert)(
            BM_BufferPool *pool,
            BM_LinkedListElement *el);
    void (*use)(
            BM_BufferPool *pool,
            BM_LinkedListElement *el);

    BM_LinkedListElement* (*elect)(BM_BufferPool *pool);

} RS_StrategyHandler;

extern
RS_StrategyHandler RS_StrategyHandlerImpl[BM_REPLACEMENT_STRAT_COUNT];

#endif //CS525_REPLACEMENT_STRATEGY_H
