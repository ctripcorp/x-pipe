package com.ctrip.xpipe.redis.core.store;

/**
 * @author Slight
 * <p>
 * Dec 04, 2021 3:59 PM
 */
public class ClusterId {

    private final Long id;

    private String mark = "cluster_";

    public static ClusterId from(Long id) {
        return new ClusterId(id);
    }

    public ClusterId(Long id) {
        this.id = id;
    }

    public ClusterId(String mark, Long id) {
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
