package com.ctrip.xpipe.redis.keeper.applier.lwm;

import com.ctrip.xpipe.tuple.Pair;

/**
 * @author Slight
 * <p>
 * Jun 03, 2022 16:39
 */
public interface Bucket {

    static Bucket create() {
        return new DefaultBucket();
    }

    Pair<Long, Boolean> add(long water);

    long lwm();
}
