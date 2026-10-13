package com.ctrip.xpipe.redis.keeper.container.resp;

import com.ctrip.xpipe.redis.keeper.storage.DeferredHandle;
import com.ctrip.xpipe.redis.keeper.storage.DeferredHandleCloser;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * {@code GET /keepers/unclosed-handles} (keeper-tfs-m6 spec §8): plain values copied from one closer
 * snapshot, so no FS or file object leaves the process and count / writeCount match the handles.
 */
public class UnclosedHandlesResp {

    private final int count;
    private final int writeCount;
    private final List<Item> handles;

    private UnclosedHandlesResp(int count, int writeCount, List<Item> handles) {
        this.count = count;
        this.writeCount = writeCount;
        this.handles = handles;
    }

    /** {@code closer} may be null (container FS not created yet): an empty result. */
    public static UnclosedHandlesResp from(DeferredHandleCloser closer, long nowMillis) {
        if (closer == null) {
            return new UnclosedHandlesResp(0, 0, Collections.emptyList());
        }
        List<Item> items = new ArrayList<>();
        int writeCount = 0;
        for (DeferredHandle handle : closer.snapshot()) {
            Item item = new Item(handle, nowMillis);
            if (item.write) {
                writeCount++;
            }
            items.add(item);
        }
        return new UnclosedHandlesResp(items.size(), writeCount, Collections.unmodifiableList(items));
    }

    public int getCount() {
        return count;
    }

    public int getWriteCount() {
        return writeCount;
    }

    public List<Item> getHandles() {
        return handles;
    }

    public static class Item {
        private final String replId;
        private final String fileKey;
        private final boolean write;
        private final String operation;
        private final String mode;
        private final long firstFailureTime;
        private final long flushDeadline;
        private final long nextRetryTime;
        private final int attempts;
        private final long age;
        private final String lastErrorType;
        private final String lastErrorMessage;

        Item(DeferredHandle handle, long nowMillis) {
            this.replId = handle.getReplIdText();
            this.fileKey = handle.getDisplayKey();
            this.write = handle.isWrite();
            this.operation = handle.getOperation();
            this.mode = handle.currentMode().name();
            this.firstFailureTime = handle.getFirstFailureTimeMillis();
            this.flushDeadline = handle.getFlushDeadlineMillis();
            this.nextRetryTime = handle.getNextRetryTimeMillis();
            this.attempts = handle.getAttempts();
            this.age = nowMillis - firstFailureTime;
            Throwable lastError = handle.getLastError();
            this.lastErrorType = lastError == null ? null : lastError.getClass().getName();
            this.lastErrorMessage = lastError == null ? null : lastError.getMessage();
        }

        public String getReplId() {
            return replId;
        }

        public String getFileKey() {
            return fileKey;
        }

        public boolean isWrite() {
            return write;
        }

        public String getOperation() {
            return operation;
        }

        public String getMode() {
            return mode;
        }

        public long getFirstFailureTime() {
            return firstFailureTime;
        }

        public long getFlushDeadline() {
            return flushDeadline;
        }

        public long getNextRetryTime() {
            return nextRetryTime;
        }

        public int getAttempts() {
            return attempts;
        }

        public long getAge() {
            return age;
        }

        public String getLastErrorType() {
            return lastErrorType;
        }

        public String getLastErrorMessage() {
            return lastErrorMessage;
        }
    }
}
