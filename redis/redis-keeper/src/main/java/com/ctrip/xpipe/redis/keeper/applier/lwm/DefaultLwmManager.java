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
import java.util.concurrent.*;
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

    @InstanceDependency
    public ScheduledExecutorService scheduled;

    @InstanceDependency
    public ExecutorService stateThread;

    @InstanceDependency
    public ExecutorService lwmThread;

    @Override
    public void doStart() throws Exception {

        scheduled.scheduleAtFixedRate(() -> {
            try {
                stateThread.submit(this::send);
            } catch (Throwable t) {
                logger.error("[send] error", t);
            }
        }, 100, 100, TimeUnit.MILLISECONDS);
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

        if (gtidSet == null) {
            logger.debug("[send] gitSet is null");
            return;
        }

        logger.debug("[send] send lwm, gtidSet {}", gtidSet);

        if (gtidSet.isEmpty() || gtidSet.isZero()) {
            logger.debug("[send] lwm {} is empty or zero skip send lwm", gtidSet);
            return;
        }

        Set<String> sids = gtidSet.getUUIDs();

        for (String sid : sids) {
            doSendLWM(sid, gtidSet.lwm(sid));
        }
    }

    public void doSendLWM(String sid, long lwm) {
        lwmThread.submit(()->{
            RedisOp redisOp = new RedisOpLwm(sid, lwm);

            try {
                new DefaultBroadcastCommand(client, redisOp).execute().addListener((f)->{
                    if (!f.isSuccess()) {
                        EventMonitor.DEFAULT.logAlertEvent("[async] failed to apply: " + redisOp.toString());
                        logger.error("[async] failed to apply: " + redisOp.toString(), f.cause());
                    }
                });
            } catch (Throwable t) {
                EventMonitor.DEFAULT.logAlertEvent("failed to apply: " + redisOp.toString());
                logger.error("failed to apply: " + redisOp.toString(), t);
            }
        });
    }

    @Override
    public void submit(String gtid) {

        logger.debug("[submit] submit lwm, gtidSet {} gtid_executed {}", gtid, gtid_executed.get());

        gtid_executed.get().add(gtid);
    }
}
