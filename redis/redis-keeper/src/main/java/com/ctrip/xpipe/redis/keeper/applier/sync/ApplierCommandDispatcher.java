package com.ctrip.xpipe.redis.keeper.applier.sync;

import com.ctrip.xpipe.gtid.GtidSet;
import com.ctrip.xpipe.redis.core.protocal.SyncObserver;
import com.ctrip.xpipe.redis.core.redis.rdb.RdbParseListener;

/**
 * @author Slight
 * <p>
 * Jun 05, 2022 18:18
 */
public interface ApplierCommandDispatcher extends SyncObserver, RdbParseListener {

    GtidSet getGtidReceived();

}
