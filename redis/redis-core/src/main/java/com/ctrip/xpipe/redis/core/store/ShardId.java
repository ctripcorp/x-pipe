package com.ctrip.xpipe.redis.core.store;

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

    public ShardId(long id) {
        this.id = id;
    }

    public ShardId(String mark, Long id) {
        this.id = id;
        this.mark = mark;
    }

    public Long id() {
        return id;
    }

    public String toString() {
        return mark + id;
    }
}
