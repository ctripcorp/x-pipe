package com.ctrip.xpipe.redis.core.server;

import com.ctrip.xpipe.gtid.GtidSet;

import java.util.List;

/**
 * @author hailu
 * @date 2024/5/14 13:55
 */
public interface FakePsyncHandler {
    // return RDB data gtid set if fsync needed, or null for partial sync
    Long handlePsync(String replId, long offset);

    byte[] genRdbData();
}
