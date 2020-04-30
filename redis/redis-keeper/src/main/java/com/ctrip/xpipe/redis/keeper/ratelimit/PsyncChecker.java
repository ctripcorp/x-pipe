package com.ctrip.xpipe.redis.keeper.ratelimit;

import com.ctrip.xpipe.redis.core.protocal.PsyncObserver;

/**
 * @author chen.zhu
 * <p>
 * Mar 02, 2020
 */
public interface PsyncChecker extends PsyncObserver {

    boolean canSendPsync();
}
