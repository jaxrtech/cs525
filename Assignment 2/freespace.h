#ifndef __FREESPACE_H__
#define __FREESPACE_H__

#include <stdint.h>
#include "dt.h"

typedef uint32_t FS_Chunk;
#define FS_ELEMENTS_PER_CHUNK (sizeof(FS_Chunk) * 8)

typedef struct FS_Freespace {
  FS_Chunk *bitmap; // array of "chunks" for the bitmap
  uint32_t chunkCount;   // number of chunks in the freespace bitmap
  uint32_t elementCount; // max number of elements (bits)
} FS_Freespace;

FS_Freespace *Freespace_create(uint32_t count);
uint32_t Freespace_markNext(FS_Freespace *self);
bool Freespace_unmark(FS_Freespace *self, uint32_t elementIndex);

#endif
