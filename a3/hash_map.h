#ifndef __HASH_MAP_H__
#define __HASH_MAP_H__

#include <stdint.h>
#include <stdbool.h>

typedef struct HS_Node {
    bool present;
    uint32_t key;
    void *data;
    struct HS_Node *next;
} HS_Node;

typedef struct HS_HashMap {
    HS_Node *buckets;
    uint32_t numBuckets;
} HS_HashMap;

HS_HashMap *HashMap_create(uint32_t numBuckets);
void HashMap_free(HS_HashMap *self);
bool HashMap_get(HS_HashMap *self, uint32_t key, void **data);
void HashMap_put(HS_HashMap *self, uint32_t key, void *data);
bool HashMap_remove(HS_HashMap *self, uint32_t key, void **data);

#endif //__HASH_SET_H__
