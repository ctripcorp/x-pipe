package com.ctrip.xpipe.redis.core.store;

import java.util.Objects;

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

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ClusterId clusterId = (ClusterId) o;
        return Objects.equals(id, clusterId.id);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id);
    }

    public String toString() {
        return mark + id;
    }
}
