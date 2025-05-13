package com.ctrip.xpipe.redis.core.store;

import java.util.Objects;

public class BacklogOffsetReplicationProgress implements ReplicationProgress<Long> {

    private long backlogOffset;

    private long endBacklogOffsetExcluded;

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

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        BacklogOffsetReplicationProgress that = (BacklogOffsetReplicationProgress) o;
        return backlogOffset == that.backlogOffset;
    }

    @Override
    public int hashCode() {
        return Objects.hash(backlogOffset);
    }

    @Override
    public String toString() {
        return String.format("backlog[%d:%d]", backlogOffset, endBacklogOffsetExcluded);
    }
}
