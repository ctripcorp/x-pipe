package com.ctrip.xpipe.redis.core.store;

import java.util.Objects;

/**
 * @author Slight
 * <p>
 * Dec 04, 2021 3:58 PM
 */
public class ShardId {

    private final Long id;

    private String mark = "shard_";

    public static ShardId from(Long id) {
        return new ShardId(id);
    }

    public ShardId(Long id) {
        this.id = id;
    }

    public ShardId(String mark, Long id) {
        this.id = id;
        this.mark = mark;
    }

    public Long id() {
        return id;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ShardId shardId = (ShardId) o;
        return Objects.equals(id, shardId.id);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id);
    }

    public String toString() {
        return mark + id;
    }
}
