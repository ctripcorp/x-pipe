package com.ctrip.xpipe.redis.console.healthcheck.nonredis.cluster;

import java.util.Set;

public interface LeveledEmbededSet<T> {

    LeveledEmbededSet<T> getSuperSet();

    LeveledEmbededSet<T> getSubSet();

    LeveledEmbededSet<T> getThrough(int level);

    Set<T> getCurrentSet();

    void add(T t);

    void remove(T t);

    void resume(T t, int level);
}
