#include "linked_list.h"
#include <stdlib.h>
#include <string.h>

BM_LinkedList *LinkedList_create(uint32_t count, size_t elementSize) {
    BM_LinkedList *list = malloc(sizeof(BM_LinkedList));
    list->count = count;
    list->elementSize = elementSize;
    list->elementsDataBuffer = calloc(count, elementSize);
    const uint32_t metaCount = count + 1; // include a sentinel head
    list->elementsMetaBuffer = calloc(metaCount, sizeof(BM_LinkedListElement));
    list->freespace = Freespace_create(count);

    // link the data buffers to the elements
    BM_LinkedListElement *els = list->elementsMetaBuffer;
    void *data = list->elementsDataBuffer;

    // setup sentinel head
    BM_LinkedListElement *sentinel = &els[0];
    list->sentinel = sentinel;
    list->head = sentinel;
    list->tail = sentinel;

    sentinel->index = 0;
    sentinel->data = NULL;
    sentinel->next = sentinel;
    sentinel->prev = sentinel;

    // setup rest of the elements
    for (uint32_t i = 1; i < metaCount; i++) {
        els[i].index = i;
        els[i].data = ((char *) data) + (elementSize * i);
    }

    return list;
}

BM_LinkedListElement *LinkedList_fetch(BM_LinkedList *self) {
    uint32_t nextIndex = Freespace_markNext(self->freespace);
    BM_LinkedListElement *el = &self->elementsMetaBuffer[nextIndex];
    memset(el->data, 0, self->elementSize);
    return el;
}

void LinkedList_unlink(BM_LinkedListElement *el) {
    el->prev->next = el->next;
    el->next->prev = el->prev;
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
    LinkedList_unlink(el);
    Freespace_unmark(self->freespace, el->index);
    return TRUE;
}

void LinkedList_prepend(
        BM_LinkedList *list,
        BM_LinkedListElement *el)
{
    LinkedList_insertAfter(el, list->sentinel);
}

void LinkedList_append(
        BM_LinkedList *list,
        BM_LinkedListElement *el)
{
    LinkedList_insertBefore(el, list->sentinel);
}


void LinkedList_insertAfter(BM_LinkedListElement *item,
                            BM_LinkedListElement *reference) {
    LinkedList_unlink(item);
    reference->next->prev = item;
    reference->prev->next = item;
    item->prev = reference;
    item->next = reference->next;
    reference->next = item;
}

void LinkedList_insertBefore(
        BM_LinkedListElement *item,
        BM_LinkedListElement *reference)
{
    BM_LinkedListElement *previous = reference->prev;
    LinkedList_insertAfter(item, previous);
}
