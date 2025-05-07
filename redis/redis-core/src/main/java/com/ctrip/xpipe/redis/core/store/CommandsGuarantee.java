package com.ctrip.xpipe.redis.core.store;

/**
 * @author lishanglin
 * date 2021/7/21
 */
public interface CommandsGuarantee {
    // guarentee cmds within [backlogOffset, INF) would be reserved
    long getBacklogOffset();

    boolean isFinish();

    boolean isTimeout();

}
