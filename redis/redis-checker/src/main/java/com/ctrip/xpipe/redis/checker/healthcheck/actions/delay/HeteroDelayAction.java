package com.ctrip.xpipe.redis.checker.healthcheck.actions.delay;

import com.ctrip.xpipe.api.foundation.FoundationService;
import com.ctrip.xpipe.redis.checker.healthcheck.RedisHealthCheckInstance;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.ping.PingService;
import com.ctrip.xpipe.utils.DateTimeUtils;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicReference;

public class HeteroDelayAction extends DelayAction {

    private AtomicReference<HeteroDelayActionContexts> contexts = new AtomicReference<>(new HeteroDelayActionContexts());

    public HeteroDelayAction(ScheduledExecutorService scheduled, RedisHealthCheckInstance instance, ExecutorService executors, PingService pingService, FoundationService foundationService) {
        super(scheduled, instance, executors, pingService, foundationService);
    }

    protected String[] getSubscribeChannel() {
        return instance.getCheckInfo().getActiveDcAllShardIds().stream().map(shardId -> "xpipe-health-check-" + foundationService.getLocalIp() + "-" + shardId).toArray(String[]::new);
    }

    @Override
    protected void onMessage(String channel, String message) {
        if (!getLifecycleState().isStarted()) {
            return;
        }
        long currentTime = System.nanoTime();
        long lastDelayPubTimeNano = Long.parseLong(message, 16);

        String[] channelParts = channel.split("-");
        Long shardId = Long.parseLong(channelParts[channelParts.length - 1]);

        HeteroDelayActionContext context = new HeteroDelayActionContext(instance, shardId, currentTime - lastDelayPubTimeNano);
        contexts.get().addContext(context);
    }

    @Override
    protected void reportDelay() {
        if (contexts.get().getContexts().isEmpty())
            return;
        contexts.get().getContexts().values().forEach(innerContext -> {
            if (isExpired(innerContext)) {
                if (!innerContext.isExpired()) {
                    innerContext.setExpired(true);
                    logger.warn("[expire][{}->{}] last update time: {}",innerContext.getShardDbId(), instance.getCheckInfo().getHostPort(),
                            DateTimeUtils.timeAsString(innerContext.getRecvTimeMilli()));
                }
                onExpired(innerContext);
            } else {
                if (innerContext.isExpired()) {
                    innerContext.setExpired(false);
                    logger.info("[expire][{}->{}] recovery",innerContext.getShardDbId(), instance.getCheckInfo().getHostPort());
                }
                onNotExpired(innerContext);
            }
        });
    }

    @Override
    protected void onExpired(DelayActionContext context) {
        notifyListeners(new HeteroDelayActionContext(instance, ((HeteroDelayActionContext) context).getShardDbId(), SAMPLE_LOST_AND_NO_PONG));
    }

    @Override
    protected void onNotExpired(DelayActionContext context) {
        notifyListeners(context);
    }

}
