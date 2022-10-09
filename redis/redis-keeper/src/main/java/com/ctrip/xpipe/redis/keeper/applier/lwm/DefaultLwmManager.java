package com.ctrip.xpipe.redis.keeper.applier.lwm;

import com.ctrip.xpipe.api.monitor.EventMonitor;
import com.ctrip.xpipe.client.redis.AsyncRedisClient;
import com.ctrip.xpipe.gtid.GtidSet;
import com.ctrip.xpipe.redis.core.redis.operation.RedisOp;
import com.ctrip.xpipe.redis.core.redis.operation.op.RedisOpLwm;
import com.ctrip.xpipe.redis.keeper.applier.AbstractInstanceComponent;
import com.ctrip.xpipe.redis.keeper.applier.InstanceDependency;
import com.ctrip.xpipe.redis.keeper.applier.command.DefaultBroadcastCommand;

import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

/**
 * @author Slight
 * <p>
 * May 30, 2022 01:43
 */
public class DefaultLwmManager extends AbstractInstanceComponent implements ApplierLwmManager {

    @InstanceDependency
    public AsyncRedisClient client;

    @InstanceDependency
    public AtomicReference<GtidSet> gtid_executed;

    public ScheduledExecutorService scheduled;

    @Override
    public void doStart() throws Exception {

        scheduled = Executors.newSingleThreadScheduledExecutor();
        scheduled.scheduleAtFixedRate(this::send, 1, 1, TimeUnit.SECONDS);
    }

    @Override
    protected void doStop() throws Exception {

        scheduled.shutdown();
        if (!scheduled.awaitTermination(3, TimeUnit.SECONDS)) {
            /* PROBABLY program does not go here */
            scheduled.shutdownNow();
        }
    }

    public void send() {

        GtidSet gtidSet = gtid_executed.get();

        Set<String> sids = gtidSet.getUUIDs();

        for (String sid : sids) {
            RedisOp redisOp = new RedisOpLwm(sid, gtidSet.lwm(sid));

            try {
                new DefaultBroadcastCommand(client, redisOp).execute().get();
            } catch (Throwable t) {
                EventMonitor.DEFAULT.logAlertEvent("failed to apply: " + redisOp.toString());
            }
        }
    }

    @Override
    public void submit(String gtid) {

        gtid_executed.get().add(gtid);
    }
}
