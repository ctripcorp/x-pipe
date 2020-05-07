package com.ctrip.xpipe.redis.meta.server.job;

import com.ctrip.xpipe.command.AbstractCommand;

/**
 * @author chen.zhu
 * <p>
 * Apr 28, 2020
 */
public abstract class AbstractMetaServerJob<V> extends AbstractCommand<V> implements MetaServerJob<V> {

}
