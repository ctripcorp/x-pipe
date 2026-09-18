package com.ctrip.xpipe.redis.keeper.storage;

import java.util.concurrent.CountDownLatch;

// Shared mutable state for all openers of the same file key.
// - state: volatile snapshot of segment offsets; segment writer mutates via COW.
// - initDone / initError: one-shot init synchronization; late openers await.
// - refCount / writerOpen: lifecycle, only touched under the striped lock.
// - initialized: whether init actually completed. A reader cannot do all init work,
// so a reader-only init leaves this false and the first writer does the rest while opening.
final class FileEntry {

    final CountDownLatch initDone = new CountDownLatch(1);
    volatile Throwable initError = null;
    volatile boolean initialized;

    volatile SegmentDirState state = SegmentDirState.EMPTY;

    int refCount;
    boolean writerOpen;
}
