package com.ctrip.xpipe.redis.core.server;

import com.ctrip.xpipe.gtid.GtidSet;

import java.util.List;

/**
 * @author lishanglin
 * date 2022/2/23
 */
public interface FakeXsyncHandler {

    // return RDB data gtid set if fsync needed, or null for partial sync
    GtidSet handleXsync(List<String> interestedSidno, GtidSet excludedGtidSet, Object excludedVectorClock);

    byte[] genRdbData();

}
