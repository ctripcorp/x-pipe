package com.ctrip.xpipe.redis.checker.healthcheck.actions.sentinel.collector.command;

import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.redis.checker.SentinelManager;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.sentinel.SentinelHello;

import java.util.HashSet;
import java.util.Set;

public class DeleteWrongSentinels extends AbstractSentinelHelloCollectCommand {

    private SentinelManager sentinelManager;

    public DeleteWrongSentinels(SentinelHelloCollectContext context, SentinelManager sentinelManager) {
        super(context);
        this.sentinelManager = sentinelManager;
    }

    @Override
    protected void doExecute() throws Throwable {
        context.getToDelete().addAll(checkWrongHellos(context.getSentinelMonitorName(), context.getSentinels(), context.getProcessedHellos()));
        new DeleteSentinels(context, sentinelManager, false).execute().addListener(deleted -> {
            context.getToDelete().clear();
            future().setSuccess();
        });
    }

    protected Set<SentinelHello> checkWrongHellos(String sentinelMonitorName, Set<HostPort> sentinels,
                                                  Set<SentinelHello> hellos) {

        Set<SentinelHello> toDelete = new HashSet<>();

        hellos.forEach((hello) -> {

            if (!hello.getMonitorName().equals(sentinelMonitorName)) {
                toDelete.add(hello);
            }
        });

        hellos.forEach((hello) -> {
            HostPort sentinel = hello.getSentinelAddr();
            if (!sentinels.contains(sentinel)) {
                toDelete.add(hello);
            }

        });

        toDelete.forEach(hellos::remove);

        return toDelete;
    }

}
