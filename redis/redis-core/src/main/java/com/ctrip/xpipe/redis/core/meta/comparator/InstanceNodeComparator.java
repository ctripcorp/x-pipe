package com.ctrip.xpipe.redis.core.meta.comparator;

import com.ctrip.xpipe.redis.core.entity.InstanceNode;

/**
 * @author ayq
 * <p>
 * 2022/4/1 22:20
 */
public class InstanceNodeComparator extends AbstractMetaComparator<Object> {
    private InstanceNode current, future;

    public InstanceNodeComparator(InstanceNode current, InstanceNode future) {
        this.current = current;
        this.future = future;
    }

    @Override
    public void compare() {
    }

    @Override
    public String idDesc() {
        return current.desc();
    }

    public InstanceNode getCurrent() {
        return current;
    }

    public InstanceNode getFuture() {
        return future;
    }
}
