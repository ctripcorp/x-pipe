package com.ctrip.xpipe.redis.core.store;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author lishanglin
 * date 2022/4/18
 */
public class OffsetReplicationProgress implements ReplicationProgress<Long> {

    // offset from store begin offset
    private AtomicLong offset;

    public OffsetReplicationProgress(long offset) {
        this.offset = new AtomicLong(offset);
    }

    @Override
    public Long getProgress() {
        return offset.get();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        OffsetReplicationProgress that = (OffsetReplicationProgress) o;
        return Objects.equals(offset, that.offset);
    }

    @Override
    public int hashCode() {
        return Objects.hash(offset);
    }

    @Override
    public String toString() {
        return String.format("offset[%d]", offset.get());
    }
}
