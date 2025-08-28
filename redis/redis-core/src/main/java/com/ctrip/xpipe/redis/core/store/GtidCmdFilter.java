package com.ctrip.xpipe.redis.core.store;

public interface GtidCmdFilter {

    boolean gtidSetContains(String uuid, long gno);
}
