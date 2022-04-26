package com.ctrip.xpipe.redis.core.meta;

public interface MetaComparatorCollector<T, K> extends MetaComparatorVisitor<T> {

    K collect();

}
