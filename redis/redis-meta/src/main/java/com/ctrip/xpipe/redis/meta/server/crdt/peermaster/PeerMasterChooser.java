package com.ctrip.xpipe.redis.meta.server.crdt.peermaster;

import com.ctrip.xpipe.api.lifecycle.Releasable;
import com.ctrip.xpipe.api.lifecycle.Startable;
import com.ctrip.xpipe.api.lifecycle.Stoppable;

public interface PeerMasterChooser extends Releasable, Startable, Stoppable {

    PeerMasterChooseCommand createMasterChooserCommand(String dcName);

}
