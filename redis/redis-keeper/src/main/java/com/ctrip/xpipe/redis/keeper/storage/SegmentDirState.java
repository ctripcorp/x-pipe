package com.ctrip.xpipe.redis.keeper.storage;

import java.util.AbstractList;
import java.util.Arrays;
import java.util.List;

import com.ctrip.xpipe.tuple.Pair;

// Immutable snapshot of a segment directory: the set of segment start offsets, in ascending order.
// Reader path reads a single volatile reference and gets an internally-consistent snapshot.
// Writer path builds a new snapshot per mutation (copy-on-write) and publishes atomically.
final class SegmentDirState {

    static final SegmentDirState EMPTY = new SegmentDirState(new long[0]);

    private final long[] offsets;    // strictly ascending
    final long firstOffset;         // -1 if empty
    final long lastOffset;          // -1 if empty

    SegmentDirState(long[] offsets) {
        this.offsets = offsets;
        this.firstOffset = offsets.length == 0 ? -1L : offsets[0];
        this.lastOffset = offsets.length == 0 ? -1L : offsets[offsets.length - 1];
    }

    boolean isEmpty() {
        return offsets.length == 0;
    }

    boolean contains(long offset) {
        return Arrays.binarySearch(offsets, offset) >= 0;
    }

    // Greatest key <= offset, or -1 if none.
    long floorKey(long offset) {
        int i = Arrays.binarySearch(offsets, offset);
        if (i >= 0) return offsets[i];
        int ins = -i - 1;
        return ins == 0 ? -1L : offsets[ins - 1];
    }

    // Greatest key <= offset and the next key after it.
    // Returns (-1, Long.MAX_VALUE) if none; next is Long.MAX_VALUE when floor is the last offset.
    Pair<Long, Long> floorKeyAndNext(long offset) {
        int i = Arrays.binarySearch(offsets, offset);
        int floorIdx;
        if (i >= 0) {
            floorIdx = i;
        } else {
            int ins = -i - 1;
            if (ins == 0) {
                return new Pair<>(-1L, Long.MAX_VALUE);
            }
            floorIdx = ins - 1;
        }
        long floor = offsets[floorIdx];
        long next = floorIdx + 1 < offsets.length ? offsets[floorIdx + 1] : Long.MAX_VALUE;
        return new Pair<>(floor, next);
    }

    int size() {
        return offsets.length;
    }

    long get(int i) {
        return offsets[i];
    }

    // Index of offset in the array, or -1 if not present.
    int indexOf(long offset) {
        int i = Arrays.binarySearch(offsets, offset);
        return i >= 0 ? i : -1;
    }

    long[] copyAppend(long value) {
        long[] next = Arrays.copyOf(offsets, offsets.length + 1);
        next[offsets.length] = value;
        return next;
    }

    long[] copyShrink(int newLength) {
        return Arrays.copyOf(offsets, newLength);
    }

    // Copy from index `from` (inclusive) to end.
    long[] copyFrom(int from) {
        return Arrays.copyOfRange(offsets, from, offsets.length);
    }

    List<Long> offsets() {
        return new AbstractList<>() {
            @Override public Long get(int index) { return offsets[index]; }
            @Override public int size() { return offsets.length; }
        };
    }
}
