#include "linked_list.h"
#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <inttypes.h>

BM_LinkedList *LinkedList_create(uint32_t count, size_t elementSize) {
    BM_LinkedList *list = malloc(sizeof(BM_LinkedList));
    list->count = count;
    list->elementSize = elementSize;
    list->elementsDataBuffer = calloc(count, elementSize);
    list->elementsMetaBuffer = calloc(count, sizeof(BM_LinkedListElement));
    list->freespace = Freespace_create(count);

    // setup sentinel head
    BM_LinkedListElement *sentinel = malloc(sizeof(BM_LinkedListElement));
    list->sentinel = sentinel;
    list->head = sentinel;
    list->tail = sentinel;

    sentinel->index = -1;
    sentinel->data = NULL;
    sentinel->next = sentinel;
    sentinel->prev = sentinel;

    // setup rest of the elements and link the data buffers to the elements
    BM_LinkedListElement *els = list->elementsMetaBuffer;
    void *data = list->elementsDataBuffer;
    for (uint32_t i = 0; i < count; i++) {
        els[i].index = i;
        els[i].data = ((char *) data) + (elementSize * i);
    }

    return list;
}

void LinkedList_free(BM_LinkedList *list) {
    if (list == NULL) { return; }

    free(list->elementsMetaBuffer);
    list->elementsMetaBuffer = NULL;
    list->head = NULL;
    list->tail = NULL;

    free(list->elementsDataBuffer);
    list->elementsDataBuffer = NULL;

    free(list->sentinel);
    list->sentinel = NULL;

    Freespace_free(list->freespace);
    free(list);
}

BM_LinkedListElement *LinkedList_fresh(BM_LinkedList *self) {
    uint32_t nextIndex;
    if (!Freespace_markNext(self->freespace, &nextIndex)) {
        return NULL;
    }
    BM_LinkedListElement *el = &self->elementsMetaBuffer[nextIndex];

#if LOG_DEBUG
    printf("DEBUG: LinkedList_fresh: el@0x%08" PRIxPTR
    " { nextIndex = %d, data = 0x%08" PRIxPTR " }\n",
           (uintptr_t) el, nextIndex, (uintptr_t) el->data);
    fflush(stdout);
#endif

    el->prev = NULL;
    el->next = NULL;
    return el;
}

void LinkedList_unlink(
        BM_LinkedList *list,
        BM_LinkedListElement *el) {

    if (el->prev) {
        el->prev->next = el->next;
    }

    if (el->next) {
        el->next->prev = el->prev;
    }

    if (list->head == el) {
        list->head = el->next;
    }

    if (list->tail == el) {
        list->tail = el->prev;
    }

    el->prev = NULL;
    el->next = NULL;
}

bool LinkedList_isEmpty(BM_LinkedList *list) {
    BM_LinkedListElement *sentinel = list->sentinel;
    return sentinel->next == sentinel
           && sentinel->prev == sentinel;
}

bool LinkedList_remove(BM_LinkedList *self, BM_LinkedListElement *el) {
    if (el == self->sentinel) {
        return FALSE;
    }
    LinkedList_unlink(self, el);
    Freespace_unmark(self->freespace, el->index);
    return TRUE;
}

bool LinkedList_replace(
        BM_LinkedList *list,
        BM_LinkedListElement *replacement,
        BM_LinkedListElement *existing)
{
    if (existing == list->sentinel) {
        return FALSE;
    }

    existing->next->prev = replacement;
    existing->prev->next = replacement;

    if (list->head == existing) {
        list->head = replacement;
    }

    if (list->tail == existing) {
        list->tail = replacement;
    }

    existing->prev = NULL;
    existing->next = NULL;
    Freespace_unmark(list->freespace, existing->index);
    return TRUE;
}

void LinkedList_prepend(
        BM_LinkedList *list,
        BM_LinkedListElement *el)
{
    LinkedList_insertAfter(list, el, list->sentinel);
}

void LinkedList_append(
        BM_LinkedList *list,
        BM_LinkedListElement *el)
{
    LinkedList_insertBefore(list, el, list->sentinel);
}


void LinkedList_insertAfter(
        BM_LinkedList *list,
        BM_LinkedListElement *item,
        BM_LinkedListElement *reference)
{
    LinkedList_unlink(list, item);
    item->prev = reference;
    item->next = reference->next;

    reference->next->prev = item;
    if (reference->next == list->sentinel) {
        list->tail = item;
        list->head = list->sentinel->next;
    }
    reference->next = item;

    if (reference == list->sentinel) {
        list->head = item;
        list->tail = list->sentinel->prev;
    }
}

void LinkedList_insertBefore(
        BM_LinkedList *list,
        BM_LinkedListElement *item,
        BM_LinkedListElement *reference)
{
    BM_LinkedListElement *previous = reference->prev;
    LinkedList_insertAfter(list, item, previous);
}
