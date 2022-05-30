package com.ctrip.xpipe.redis.keeper.applier.sequence;

import com.ctrip.xpipe.exception.XpipeRuntimeException;
import com.ctrip.xpipe.lifecycle.AbstractLifecycle;

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
public class DefaultLwmManager extends AbstractLifecycle implements ApplierLwmManager {

    public Map<String, Long> lwms = new ConcurrentHashMap<>();

    public ApplierSequenceController sequence;

    public ScheduledExecutorService scheduled;

    public DefaultLwmManager(ApplierSequenceController sequence) {
        this.sequence = sequence;
    }

    @Override
    public void start() throws Exception {
        scheduled = Executors.newSingleThreadScheduledExecutor();

        // periodically: sequence.submit(/* gtid.lwm [sourceId] [lwm] */);
    }

    @Override
    protected void doStop() throws Exception {

        scheduled.shutdown();
        if (!scheduled.awaitTermination(3, TimeUnit.SECONDS)) {
            /* PROBABLY program does not go here */
            scheduled.shutdownNow();
        }
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
