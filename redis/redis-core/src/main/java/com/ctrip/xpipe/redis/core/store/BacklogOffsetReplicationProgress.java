package com.ctrip.xpipe.redis.core.store;

import java.util.Objects;

public class BacklogOffsetReplicationProgress implements ReplicationProgress<Long> {

    private long backlogOffset;

    public BacklogOffsetReplicationProgress(long backlogOffset) {
        this.backlogOffset = backlogOffset;
    }

    @Override
    public Long getProgress() {
        return backlogOffset;
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
        return String.format("backlog[%d]", backlogOffset);
    }
}
