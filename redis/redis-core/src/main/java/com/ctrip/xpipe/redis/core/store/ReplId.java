package com.ctrip.xpipe.redis.core.store;

import java.util.Objects;

/**
 * @author lishanglin
 * date 2023/10/31
 */
public class ReplId {

    private final Long id;

    private String mark = "repl_";

    public static ReplId from(Long id) {
        return new ReplId(id);
    }

    public ReplId(Long id) {
        this.id = id;
    }

    public ReplId(String mark, Long id) {
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
        ReplId replId = (ReplId) o;
        return Objects.equals(id, replId.id);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id);
    }

    public String toString() {
        return mark + id;
    }
}
