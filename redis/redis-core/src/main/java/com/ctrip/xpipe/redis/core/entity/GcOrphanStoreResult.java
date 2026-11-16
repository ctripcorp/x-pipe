package com.ctrip.xpipe.redis.core.entity;

/**
 * Result of one {@code gcOrphanStore} attempt on a single {@code repl_<replId>} directory, reported
 * back to the metaserver's periodic orphan-store GC round for logging/CAT only — the keeper container
 * is the sole authority on whether a delete actually happened.
 */
public class GcOrphanStoreResult {

    public enum Status {
        /** store_manager_meta.properties deleted and the rest of the directory removed (best effort). */
        DELETED,
        /** nothing there to begin with (already gone, e.g. a previous round or a race). */
        NOT_FOUND,
        /** left alone: too fresh, meta not openable, or still locally in use — never an error. */
        SKIPPED,
        /** deleting store_manager_meta.properties itself failed; directory left untouched. */
        FAILED
    }

    public Long replId;

    public Status status;

    public String reason;

    public GcOrphanStoreResult() {
    }

    public GcOrphanStoreResult(Long replId, Status status, String reason) {
        this.replId = replId;
        this.status = status;
        this.reason = reason;
    }

    public static GcOrphanStoreResult notFound(Long replId) {
        return new GcOrphanStoreResult(replId, Status.NOT_FOUND, "nothing to gc");
    }

    public static GcOrphanStoreResult skip(Long replId, String reason) {
        return new GcOrphanStoreResult(replId, Status.SKIPPED, reason);
    }

    public static GcOrphanStoreResult deleted(Long replId) {
        return new GcOrphanStoreResult(replId, Status.DELETED, null);
    }

    public static GcOrphanStoreResult fail(Long replId, String reason) {
        return new GcOrphanStoreResult(replId, Status.FAILED, reason);
    }

    @Override
    public String toString() {
        return "GcOrphanStoreResult{" +
                "replId=" + replId +
                ", status=" + status +
                ", reason='" + reason + '\'' +
                '}';
    }
}
