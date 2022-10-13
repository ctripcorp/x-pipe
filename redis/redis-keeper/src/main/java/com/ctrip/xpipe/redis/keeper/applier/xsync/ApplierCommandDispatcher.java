package com.ctrip.xpipe.redis.keeper.applier.xsync;

import com.ctrip.xpipe.redis.core.protocal.XsyncObserver;
import com.ctrip.xpipe.redis.core.redis.rdb.RdbParseListener;

/**
 * @author Slight
 * <p>
 * Jun 05, 2022 18:18
 */
public interface ApplierCommandDispatcher extends XsyncObserver, RdbParseListener {

}
