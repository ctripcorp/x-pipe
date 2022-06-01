package com.ctrip.xpipe.redis.keeper.applier.sequence;

import com.ctrip.xpipe.client.redis.AsyncRedisClient;
import com.ctrip.xpipe.exception.XpipeRuntimeException;
import com.ctrip.xpipe.lifecycle.AbstractLifecycle;
import com.ctrip.xpipe.redis.core.redis.operation.RedisOp;
import com.ctrip.xpipe.redis.core.redis.operation.op.RedisOpLwm;
import com.ctrip.xpipe.redis.keeper.applier.AbstractInstanceComponent;
import com.ctrip.xpipe.redis.keeper.applier.InstanceDependency;
import com.ctrip.xpipe.redis.keeper.applier.command.DefaultApplierCommand;
import com.google.common.collect.Lists;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * @author Slight
 * <p>
 * May 30, 2022 01:43
 */
public class DefaultLwmManager extends AbstractInstanceComponent implements ApplierLwmManager {

    @InstanceDependency
    public ApplierSequenceController sequence;

    @InstanceDependency
    public AsyncRedisClient client;

    public Map<String, Long> lwms = new ConcurrentHashMap<>();

    public ScheduledExecutorService scheduled;

    @Override
    public void doStart() throws Exception {

        scheduled = Executors.newSingleThreadScheduledExecutor();
        scheduled.scheduleAtFixedRate(()->lwms.forEach(this::send), 1, 1, TimeUnit.SECONDS);
    }

    @Override
    protected void doStop() throws Exception {

        scheduled.shutdown();
        if (!scheduled.awaitTermination(3, TimeUnit.SECONDS)) {
            /* PROBABLY program does not go here */
            scheduled.shutdownNow();
        }
    }

    public void send(String sid, Long lwm) {

        RedisOp redisOp = new RedisOpLwm(Lists.newArrayList("gtid.lwm", sid, lwm.toString()));
        sequence.submit(new DefaultApplierCommand(client, redisOp));
    }

    @Override
    public void submit(String gtid) {

        String[] split = gtid.split(":");
        if (split.length != 2) {
            throw new XpipeRuntimeException("DefaultLwmManager.submit() - invalid gtid: " + gtid);
        }

        String sourceId = split[0];
        long transactionId = Long.parseLong(split[1]);

        Long current = lwms.get(sourceId);
        long lwm = current == null ? 0L : current;

        if (transactionId > lwm) {
            lwms.put(sourceId, transactionId);
        }
    }
}
