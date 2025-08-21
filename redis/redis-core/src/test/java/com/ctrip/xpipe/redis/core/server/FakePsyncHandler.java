package com.ctrip.xpipe.redis.core.server;

/**
 * @author hailu
 * @date 2024/5/14 13:55
 */
public interface FakePsyncHandler {
    // return RDB data gtid set if fsync needed, or null for partial sync
    Long handlePsync(String replId, long offset);

    byte[] genRdbData();
}
