package com.ctrip.xpipe.redis.meta.server.meta;

/**
 * @author ayq
 * <p>
 * 2022/10/20 11:14
 */
public interface CurrentShardInstanceMeta {

    boolean watchIfNotWatched();
}
