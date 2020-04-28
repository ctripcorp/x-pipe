package com.ctrip.xpipe.redis.meta.server.job;

import com.ctrip.xpipe.api.command.Command;

/**
 * @author chen.zhu
 * <p>
 * Apr 28, 2020
 */
public interface MetaServerJob<V> extends Command<V> {
    boolean isSerial();
}
