package com.ctrip.xpipe.redis.core.store;

import com.ctrip.xpipe.gtid.GtidSet;

public class XSyncContinue {

    // gtidset where repl stream start
    GtidSet continueGtidSet;

    // offset of cmd files
    long backlogOffset;

    public XSyncContinue(GtidSet continueGtidSet, long backlogOffset) {
        this.continueGtidSet = continueGtidSet;
        this.backlogOffset = backlogOffset;
    }

    public GtidSet getContinueGtidSet() {
        return continueGtidSet;
    }

    public long getBacklogOffset() {
        return backlogOffset;
    }

}
