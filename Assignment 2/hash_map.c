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

// Hash function for 32-bit integers
// see https://stackoverflow.com/a/12996028/809572
static uint32_t hash(uint32_t x) {
    x = ((x >> 16) ^ x) * 0x45d9f3b;
    x = ((x >> 16) ^ x) * 0x45d9f3b;
    x = (x >> 16) ^ x;
    return x;
}

bool HashMap_get(HS_HashMap *self, uint32_t key, void **data) {
    *data = NULL;
    uint32_t i = hash(key) % self->numBuckets;
    HS_Node *node = &self->buckets[i];
    while (node != NULL && node->key != key) {
        node = node->next;
    }

    if (node == NULL) {
        return FALSE;
    }

    *data = node->data;
    return TRUE;
}

void HashMap_put(HS_HashMap *self, uint32_t key, void *data) {
    uint32_t i = hash(key) % self->numBuckets;
    bool isRoot = true;
    HS_Node *node = &self->buckets[i];
    HS_Node *lookahead = node->next;
    while (lookahead != NULL) {
        isRoot = false;
        node = lookahead;
        lookahead = node->next;
    }

    if (!isRoot) {
        node = malloc(sizeof(HS_Node));
    }

    node->key = key;
    node->data = data;
    node->next = NULL;
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
    while (lookahead != NULL) {
        if (node->key == key) {
            found = true;
            break;
        }
        isRoot = false;
        prev = node;
        node = lookahead;
        lookahead = node->next;
    }

    if (!found) {
        return false;
    }

    if (!isRoot) {
        free(node);
        prev->next = NULL;
    }

    if (data != NULL) {
        *data = node->data;
    }

    return true;
}