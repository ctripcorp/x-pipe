package com.ctrip.xpipe.redis.core.store;

import java.util.Objects;

public class BacklogOffsetReplicationProgress implements ReplicationProgress<Long> {

    private long backlogOffset;

    private long endBacklogOffset;

    public BacklogOffsetReplicationProgress(long backlogOffset) {
        this(backlogOffset, -1);
    }

    public BacklogOffsetReplicationProgress(long backlogOffset, long endBacklogOffset) {
        this.backlogOffset = backlogOffset;
        this.endBacklogOffset = endBacklogOffset;
    }

    @Override
    public Long getProgress() {
        return backlogOffset;
    }

    public void setEndBacklogOffset(long endBacklogOffset) {
        this.endBacklogOffset = endBacklogOffset;
    }

    public Long getEndProgress() {
        return endBacklogOffset;
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
        return String.format("backlog[%d:%d]", backlogOffset, endBacklogOffset);
    }
}
