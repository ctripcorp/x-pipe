package com.ctrip.xpipe.redis.keeper.storage;

import com.ctrip.xpipe.redis.core.store.ReplId;

/**
 * A file handle whose close failed and that {@link DeferredHandleCloser} keeps retrying
 * (keeper-tfs-m6 spec §2.1). Fixed fields are set on defer; the mutable ones are written only by
 * the closer worker and are volatile so snapshot readers see a recent value of each field.
 */
public final class DeferredHandle {

    public enum CloseMode {
        FLUSH,
        NO_FLUSH
    }

    private static final char KEY_SEPARATOR = '\0';
    private static final char KEY_SEPARATOR_DISPLAY = '|';
    static final String UNKNOWN_REPL_ID = "unknown";
    private static final long NEVER_LOGGED = -1L;

    final AsyncFileSystem fs;
    final AbstractStorageFile file;
    // Passed in by the close caller; FS file objects do not carry it.
    private final ReplId replId;
    private final boolean write;
    private final String operation;
    private final long firstFailureTimeMillis;
    private final long flushDeadlineMillis;

    private volatile boolean degraded;
    private volatile int attempts;
    private volatile long nextRetryTimeMillis;
    private volatile Throwable lastError;
    // Consecutive failures in the current close mode, the Helper's failure included. Drives the
    // backoff and is reset when the handle degrades to noFlush.
    private volatile int backoffFailures;
    // Time of the last logged retry failure, NEVER_LOGGED before the first; worker only.
    private long lastRetryFailureLogMillis = NEVER_LOGGED;

    DeferredHandle(AsyncFileSystem fs, AbstractStorageFile file, ReplId replId, String operation, Throwable cause,
                   long nowMillis, long flushWindowMillis) {
        this.fs = fs;
        this.file = file;
        this.replId = replId;
        this.write = file.canWrite();
        this.operation = operation;
        this.firstFailureTimeMillis = nowMillis;
        this.flushDeadlineMillis = write ? nowMillis + Math.max(0L, flushWindowMillis) : 0L;
        this.lastError = cause;
        this.backoffFailures = 1;
    }

    /** Close mode of the next attempt; read handles and degraded write handles never flush. */
    public CloseMode currentMode() {
        return write && !degraded ? CloseMode.FLUSH : CloseMode.NO_FLUSH;
    }

    /** FS file key with the NUL separators rendered as {@code |}, e.g. {@code f|/path}. */
    public String getDisplayKey() {
        return displayKey(file.getKey());
    }

    static String displayKey(String key) {
        return key == null ? null : key.replace(KEY_SEPARATOR, KEY_SEPARATOR_DISPLAY);
    }

    String getFileKey() {
        return file.getKey();
    }

    /** May be null when the caller had none; see {@link #replIdText(ReplId)}. */
    public ReplId getReplId() {
        return replId;
    }

    /** ReplId for display, {@code unknown} when null. */
    public String getReplIdText() {
        return replIdText(replId);
    }

    static String replIdText(ReplId replId) {
        return replId == null ? UNKNOWN_REPL_ID : replId.toString();
    }

    public boolean isWrite() {
        return write;
    }

    public String getOperation() {
        return operation;
    }

    public long getFirstFailureTimeMillis() {
        return firstFailureTimeMillis;
    }

    public long getFlushDeadlineMillis() {
        return flushDeadlineMillis;
    }

    public boolean isDegraded() {
        return degraded;
    }

    public int getAttempts() {
        return attempts;
    }

    public long getNextRetryTimeMillis() {
        return nextRetryTimeMillis;
    }

    public Throwable getLastError() {
        return lastError;
    }

    void markDegraded() {
        degraded = true;
        backoffFailures = 0;
    }

    /** Records a failed attempt and returns the consecutive failure count used for the backoff. */
    int recordFailure(Throwable error) {
        attempts++;
        lastError = error;
        return ++backoffFailures;
    }

    int getBackoffFailures() {
        return backoffFailures;
    }

    void setNextRetryTimeMillis(long nextRetryTimeMillis) {
        this.nextRetryTimeMillis = nextRetryTimeMillis;
    }

    /**
     * Rate limit of the retry failure log (spec §8): true for the first failure and then at most once
     * per {@code intervalMillis}; a true result counts as logged.
     */
    boolean tryLogRetryFailure(long nowMillis, long intervalMillis) {
        if (lastRetryFailureLogMillis != NEVER_LOGGED && nowMillis - lastRetryFailureLogMillis < intervalMillis) {
            return false;
        }
        lastRetryFailureLogMillis = nowMillis;
        return true;
    }

    /** {@code Type: message} without the stack trace, for the expected close failures (spec §8). */
    static String errorSummary(Throwable error) {
        if (error == null) {
            return null;
        }
        return error.getClass().getSimpleName() + ": " + error.getMessage();
    }

    /** Log form of the entry (spec §8), with the age at {@code nowMillis}. */
    String describe(long nowMillis) {
        return "repl_id=" + replIdText(replId) + " fileKey=" + getDisplayKey() + " write=" + write
                + " attempts=" + attempts + " age=" + (nowMillis - firstFailureTimeMillis) + "ms mode=" + currentMode()
                + " operation=" + operation;
    }

    @Override
    public String toString() {
        return "DeferredHandle{repl_id=" + replIdText(replId) + ", fileKey=" + getDisplayKey() + ", write=" + write
                + ", attempts=" + attempts + ", mode=" + currentMode() + ", operation=" + operation + "}";
    }
}
