#include "hash_map.h"
#include "dt.h"
#include <stdlib.h>
#include <stdbool.h>

HS_HashMap *HashMap_create(uint32_t numBuckets) {
    HS_HashMap *self = malloc(sizeof(HS_HashMap));
    self->numBuckets = numBuckets;
    self->buckets = calloc(numBuckets, sizeof(HS_Node));
    return self;
}

void HashMap_free(HS_HashMap *self) {
    if (self == NULL) { return; }

    // free any overflow nodes
    for (uint32_t i = 0; i < self->numBuckets; i++) {
        HS_Node *cur = &self->buckets[i];
        if (!cur->present) {
            continue;
        }

        cur = cur->next;
        HS_Node *tmp;
        while (cur != NULL) {
            tmp = cur->next;
            free(cur);
            cur = tmp;
        }
    }

    free(self->buckets);
    self->buckets = NULL;

    free(self);
}

// Hash function for 32-bit integers
// see https://stackoverflow.com/a/12996028/809572
static uint32_t hash(uint32_t x) {
    x = ((x >> 16u) ^ x) * 0x45d9f3b;
    x = ((x >> 16u) ^ x) * 0x45d9f3b;
    x = (x >> 16u) ^ x;
    return x;
}

bool HashMap_get(HS_HashMap *self, uint32_t key, void **data) {
    *data = NULL;
    uint32_t i = hash(key) % self->numBuckets;
    HS_Node *node = &self->buckets[i];
    while (node != NULL && node->key != key) {
        node = node->next;
    }

    if (!(node != NULL
          && node->present
          && node->key == key)) {
        return FALSE;
    }

    *data = node->data;
    return TRUE;
}

void HashMap_put(HS_HashMap *self, uint32_t key, void *data) {
    uint32_t i = hash(key) % self->numBuckets;
    HS_Node *node = &self->buckets[i];

    bool isRoot =
            (node->present && node->key == key)
            || (!node->present);

    if (isRoot) {
        node->present = TRUE;
        node->key = key;
        node->data = data;
        return;
    }

    // Continue to iterate the linked list, unless we find a matching key
    // or we hit the end
    HS_Node *lookahead = node->next;
    while (node->present && lookahead != NULL) {
        node = lookahead;
        if (node->present && node->key == key) {
            node->data = data;
            return;
        }
        lookahead = node->next;
    }

    // We hit the end and did not find the key
    // Create a new one at the end
    HS_Node *last = malloc(sizeof(HS_Node));
    node->next = last;

    last->present = TRUE;
    last->key = key;
    last->data = data;
    last->next = NULL;
}

bool HashMap_remove(HS_HashMap *self, uint32_t key, void **data) {
    if (data != NULL) {
        *data = NULL;
    }

    uint32_t i = hash(key) % self->numBuckets;
    bool isRoot = true;
    bool found = false;
    HS_Node *prev = NULL;
    HS_Node *node = &self->buckets[i];
    HS_Node *lookahead = node->next;
    do {
        if (node->present && node->key == key) {
            found = true;
            break;
        }
        isRoot = false;
        prev = node;
        node = lookahead;
        lookahead = node->next;
    } while (lookahead != NULL);

    if (!found) {
        return false;
    }

    if (data != NULL) {
        *data = node->data;
    }

    node->present = FALSE;
    if (isRoot) {
        HS_Node *next = node->next;
        if (next != NULL) {
            // Copy the next value since the root is persistent
            *node = *next;
            free(next);
        } else {
            node->present = FALSE;
            node->next = NULL;
            node->data = NULL;
            node->key = -1;
        }
    } else {
        prev->next = node->next;
        free(node);
    }

    return true;
}