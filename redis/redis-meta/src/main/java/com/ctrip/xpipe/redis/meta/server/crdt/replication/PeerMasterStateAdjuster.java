package com.ctrip.xpipe.redis.meta.server.crdt.replication;

import com.ctrip.xpipe.api.lifecycle.Releasable;
import com.ctrip.xpipe.api.lifecycle.Startable;
import com.ctrip.xpipe.api.lifecycle.Stoppable;

public interface PeerMasterStateAdjuster extends Releasable, Startable, Stoppable {

}
