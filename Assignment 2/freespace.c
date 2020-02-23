#include "freespace.h"
#include "dt.h"

// Use de Bruijn multiplication to find next available bit in the free-space bitmap
// see http://supertech.csail.mit.edu/papers/debruijn.pdf, https://stackoverflow.com/a/31718095/809572
//
static uint8_t lsb(uint32_t v) {
    static const int MultiplyDeBruijnBitPosition[32] = {
            0, 9, 1, 10, 13, 21, 2, 29, 11, 14, 16, 18, 22, 25, 3, 30,
            8, 12, 20, 28, 15, 17, 24, 7, 19, 27, 23, 6, 26, 5, 4, 31
    };

    v |= v >> 1u; // first round down to one less than a power of 2
    v |= v >> 2u;
    v |= v >> 4u;
    v |= v >> 8u;
    v |= v >> 16u;

    return MultiplyDeBruijnBitPosition[(uint32_t)(v * 0x07C4ACDDU) >> 27u];
}

uint32_t Freespace_markNext(FS_Freespace *self) {
    const uint32_t chunkCount = self->chunkCount;
    const uint32_t elementCount = self->elementCount;
    uint32_t *bitmap = self->bitmap;

    for (uint32_t i = 0; i < chunkCount; i++) {
        uint32_t chunk = ~bitmap[i];
        if (chunk == 0) { continue; }

        uint8_t b = lsb(chunk);
        int blk = (int)
              ((i * FS_ELEMENTS_PER_CHUNK)
            + ((FS_ELEMENTS_PER_CHUNK - 1) - b));

        if (blk > elementCount) {
            return -1;
        }
        bitmap[i] = chunk & (1u << b);
        return blk;
    }

    return -1;
}

bool Freespace_unmark(FS_Freespace *self, uint32_t elementIndex) {
    FS_Chunk *bitmap = self->bitmap;
    uint32_t maxIndex = self->elementCount - 1;
    if (elementIndex > maxIndex) {
        return FALSE;
    }

    uint32_t i = (elementIndex / FS_ELEMENTS_PER_CHUNK);
    uint32_t b = (elementIndex % FS_ELEMENTS_PER_CHUNK);
    uint32_t chunk = bitmap[i];
    bitmap[i] = chunk & ~(1u << b);
    return TRUE;
}
