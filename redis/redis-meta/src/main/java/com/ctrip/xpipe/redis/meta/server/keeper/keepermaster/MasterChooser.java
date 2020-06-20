package com.ctrip.xpipe.redis.meta.server.keeper.keepermaster;

import com.ctrip.xpipe.api.lifecycle.Releasable;
import com.ctrip.xpipe.api.lifecycle.Startable;
import com.ctrip.xpipe.api.lifecycle.Stoppable;

/**
 * @author wenchao.meng
 *
 * Nov 4, 2016
 */
public interface MasterChooser extends Releasable, Startable, Stoppable {

}
