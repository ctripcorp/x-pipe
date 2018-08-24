package com.ctrip.xpipe.redis.console.healthcheck.redis;

import com.ctrip.xpipe.api.lifecycle.Startable;
import com.ctrip.xpipe.api.lifecycle.Stoppable;
import com.ctrip.xpipe.api.server.Server;
import javafx.scene.paint.Stop;

/**
 * @author chen.zhu
 * <p>
 * Aug 27, 2018
 */
public interface RedisContext extends Startable, Stoppable {

    boolean isMater();

    long getReplOffset();

    Server.SERVER_ROLE getRole();

    void refresh();
}
