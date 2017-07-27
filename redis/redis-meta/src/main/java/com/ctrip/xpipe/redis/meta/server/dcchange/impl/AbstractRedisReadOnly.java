package com.ctrip.xpipe.redis.meta.server.dcchange.impl;

import java.util.concurrent.ScheduledExecutorService;

import com.ctrip.xpipe.redis.meta.server.dcchange.exception.RedisReadonlyException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ctrip.xpipe.api.command.Command;
import com.ctrip.xpipe.pool.XpipeNettyClientKeyedObjectPool;
import com.ctrip.xpipe.redis.meta.server.dcchange.RedisReadonly;

/**
 * @author wenchao.meng
 *         <p>
 *         Feb 24, 2017
 */
public abstract class AbstractRedisReadOnly implements RedisReadonly {

    protected Logger logger = LoggerFactory.getLogger(getClass());

    protected String ip;

    protected int port;

    protected XpipeNettyClientKeyedObjectPool keyedObjectPool;

    protected ScheduledExecutorService scheduled;

    public AbstractRedisReadOnly(String ip, int port, XpipeNettyClientKeyedObjectPool keyedObjectPool, ScheduledExecutorService scheduled) {

        this.ip = ip;
        this.port = port;
        this.keyedObjectPool = keyedObjectPool;
        this.scheduled = scheduled;
    }


    @Override
    public void makeReadOnly() throws RedisReadonlyException {

        try {
            Command<?> command = createReadOnlyCommand();
            Object result = command.execute().get();
            logger.info("[makeReadOnly]{}:{}, {}", ip, port, result);
        } catch (Exception e) {
            throw new RedisReadonlyException(String.format("%s:%d make readonly", ip, port), e);
        }
    }

    protected abstract Command<?> createReadOnlyCommand();


    @Override
    public void makeWritable() throws RedisReadonlyException {
        try {
            Command<?> command = createWritableCommand();
            Object result = command.execute().get();
            logger.info("[makeWritable]{}:{}, {}", ip, port, (Object) result);
        } catch (Exception e) {
            throw new RedisReadonlyException(String.format("%s:%d makeWritable", ip, port), e);
        }
    }


    protected abstract Command<?> createWritableCommand();

}
