#include "replacement_strategy.h"

static void RS_FIFO_insert(
        BM_BufferPool *pool,
        BM_LinkedListElement *el)
{
    BP_Metadata *meta = pool->mgmtData;
    LinkedList_prepend(meta->pageTable, el);
}

static BM_LinkedListElement* RS_FIFO_elect(
        BM_BufferPool *pool) {
    BP_Metadata *meta = pool->mgmtData;
    return meta->pageTable->head;
}

//
// Least recently used (LRU)
//
//   - Keeps the most recently used closer to the head while least used elements
//     are closer to the tail
//

static void RS_LRU_insert(
        BM_BufferPool *pool,
        BM_LinkedListElement *el)
{
    BP_Metadata *meta = pool->mgmtData;
    LinkedList_prepend(meta->pageTable, el);
}

static void RS_LRU_use(
        BM_BufferPool *pool,
        BM_LinkedListElement *el)
{
    // TODO
}

static BM_LinkedListElement* RS_LRU_elect(
        BM_BufferPool *pool) {
    BP_Metadata *meta = pool->mgmtData;
    return meta->pageTable->head;
}

//

RS_StrategyHandler
        RS_StrategyHandlerImpl[BM_REPLACEMENT_STRAT_COUNT] = {
        [RS_FIFO] = {
                .strategy = RS_FIFO,
                .insert = RS_FIFO_insert,
                .use = NULL,
                .elect = RS_FIFO_elect,
        },
        [RS_CLOCK] = NULL,
        [RS_LFU] = NULL,
        [RS_LRU] = {
                .strategy = RS_LRU,
                .insert = RS_LRU_insert,
                .use = RS_LRU_use,
                .elect = RS_LRU_elect,
        },
        [RS_LRU_K] = NULL,
};
