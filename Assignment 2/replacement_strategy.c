#include "replacement_strategy.h"
#include <stdlib.h>

typedef struct RS_FIFO_Metadata {
    BM_LinkedListElement *next;
} RS_FIFO_Metadata;

static void RS_FIFO_init(
        BM_BufferPool *pool)
{
    BP_Metadata *meta = pool->mgmtData;
    
    meta->strategyMetadata = malloc(sizeof(RS_FIFO_Metadata));
    RS_FIFO_Metadata *rs = meta->strategyMetadata;
    rs->next = NULL;
}

static void RS_FIFO_insert(
        BM_BufferPool *pool,
        BM_LinkedListElement *el)
{
    BP_Metadata *meta = pool->mgmtData;
    LinkedList_append(meta->pageDescriptors, el);
}

static void RS_FIFO_use(
        BM_BufferPool *pool,
        BM_LinkedListElement *el)
{
    // empty
}

static BM_LinkedListElement* RS_FIFO_elect(
        BM_BufferPool *pool) {
    BP_Metadata *meta = pool->mgmtData;
    RS_FIFO_Metadata *rs = meta->strategyMetadata;

    BM_LinkedList *list = meta->pageDescriptors;
    BM_LinkedListElement *el = rs->next;
    if (el == NULL) {
        el = list->head;
    }
    
    BM_LinkedListElement *next = el->next;
    if (next == list->sentinel) {
        // we've reached the end, signal to just get the `head` next time
        next = NULL;
    }
    rs->next = next;

    return el;
}

//
// Least recently used (LRU)
//
//   - Keeps the most recently used closer to the head while least used elements
//     are closer to the tail
//

static void RS_LRU_init(BM_BufferPool *pool)
{
    // TODO
}

static void RS_LRU_insert(
        BM_BufferPool *pool,
        BM_LinkedListElement *el)
{
    BP_Metadata *meta = pool->mgmtData;
    LinkedList_prepend(meta->pageDescriptors, el);
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
    return meta->pageDescriptors->head;
}

//

RS_StrategyHandler
        RS_StrategyHandlerImpl[BM_REPLACEMENT_STRAT_COUNT] = {
        [RS_FIFO] = {
                .strategy = RS_FIFO,
                .init = RS_FIFO_init,
                .insert = RS_FIFO_insert,
                .use = RS_FIFO_use,
                .elect = RS_FIFO_elect,
        },
        [RS_CLOCK] = NULL,
        [RS_LFU] = NULL,
        [RS_LRU] = {
                .strategy = RS_LRU,
                .init = RS_FIFO_init,
                .insert = RS_LRU_insert,
                .use = RS_LRU_use,
                .elect = RS_LRU_elect,
        },
        [RS_LRU_K] = NULL,
};
