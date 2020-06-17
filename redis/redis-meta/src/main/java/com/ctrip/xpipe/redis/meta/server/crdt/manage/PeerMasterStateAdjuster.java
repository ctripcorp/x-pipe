package com.ctrip.xpipe.redis.meta.server.crdt.manage;

import com.ctrip.xpipe.api.lifecycle.Releasable;
import com.ctrip.xpipe.api.lifecycle.Startable;
import com.ctrip.xpipe.api.lifecycle.Stoppable;

public interface PeerMasterStateAdjuster extends Releasable, Startable, Stoppable {

    void adjust();

    void clearPeerMaster();

}
