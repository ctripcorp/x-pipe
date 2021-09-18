package com.ctrip.xpipe.redis.core.meta;


public interface MetaSynchronizer {
    String META_SYNC = "meta.sync";
    void sync();
}
