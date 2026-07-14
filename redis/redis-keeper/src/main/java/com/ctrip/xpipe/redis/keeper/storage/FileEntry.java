package com.ctrip.xpipe.redis.keeper.storage;

import java.util.concurrent.CountDownLatch;

// Shared mutable state for all openers of the same file key.
// - state: volatile snapshot of segment offsets; segment writer mutates via COW.
// - initDone / initError: one-shot init synchronization; late openers await.
// - refCount / writerOpen: lifecycle, only touched under the striped lock.
final class FileEntry {

    final CountDownLatch initDone = new CountDownLatch(1);
    volatile boolean initFailed = false;

    volatile SegmentDirState state = SegmentDirState.EMPTY;

    int refCount;
    boolean writerOpen;
}
