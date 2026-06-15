package com.ctrip.xpipe.redis.core.store;

import com.ctrip.xpipe.gtid.GtidSet;

import java.util.Objects;

public class BacklogOffsetReplicationProgress implements ReplicationProgress<Long> {

    private long backlogOffset;

    private long endBacklogOffsetExcluded;

    private GtidSet rdbGtidExecuted;

    public BacklogOffsetReplicationProgress(long backlogOffset) {
        this(backlogOffset, -1);
    }

    public BacklogOffsetReplicationProgress(long backlogOffset, long endBacklogOffsetExcluded) {
        this.backlogOffset = backlogOffset;
        this.endBacklogOffsetExcluded = endBacklogOffsetExcluded;
    }

    @Override
    public Long getProgress() {
        return backlogOffset;
    }

    public void setEndBacklogOffsetExcluded(long endBacklogOffsetExcluded) {
        this.endBacklogOffsetExcluded = endBacklogOffsetExcluded;
    }

    public Long getEndProgressExcluded() {
        return endBacklogOffsetExcluded;
    }

    public void setRdbGtidExecuted(GtidSet rdbGtidExecuted) {
        this.rdbGtidExecuted = rdbGtidExecuted == null ? null : rdbGtidExecuted.clone();
    }

    public GtidSet getRdbGtidExecuted() {
        return rdbGtidExecuted == null ? null : rdbGtidExecuted.clone();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        BacklogOffsetReplicationProgress that = (BacklogOffsetReplicationProgress) o;
        return backlogOffset == that.backlogOffset
                && endBacklogOffsetExcluded == that.endBacklogOffsetExcluded
                && Objects.equals(rdbGtidExecuted, that.rdbGtidExecuted);
    }

    @Override
    public int hashCode() {
        return Objects.hash(backlogOffset, endBacklogOffsetExcluded, rdbGtidExecuted);
    }

    @Override
    public String toString() {
        return String.format("backlog[%d:%d]", backlogOffset, endBacklogOffsetExcluded);
    }
}
