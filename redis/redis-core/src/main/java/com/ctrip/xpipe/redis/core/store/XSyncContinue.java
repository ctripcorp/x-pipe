package com.ctrip.xpipe.redis.core.store;

import com.ctrip.xpipe.gtid.GtidSet;

public class XSyncContinue {

    // gtidset where repl stream start
    GtidSet continueGtidSet;

    // gtidset of cmds in backlog which will be sent later
    GtidSet backlogGtidSet;

    // offset of cmd files
    long backlogOffset;

    public XSyncContinue(GtidSet continueGtidSet, GtidSet backlogGtidSet, long backlogOffset) {
        this.continueGtidSet = continueGtidSet;
        this.backlogGtidSet = backlogGtidSet;
        this.backlogOffset = backlogOffset;
    }

}
