package com.ctrip.xpipe.redis.checker.healthcheck.actions.sentinel.collector.command;

import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.monitor.CatEventMonitor;
import com.ctrip.xpipe.redis.checker.healthcheck.actions.sentinel.SentinelHello;

import java.util.*;

import static com.ctrip.xpipe.redis.checker.healthcheck.actions.sentinel.SentinelHelloCheckAction.LOG_TITLE;

public class CheckWrongSentinels4BackupDc extends AbstractSentinelHelloCollectCommand {

    private Set<HostPort> allSentinels;
    private static final String WRONG_OTHER_SHARD = "wrong-sentinel-of-other-shard";
    private static final String WRONG_OUT_OF_XPIPE = "wrong-sentinel-out-of-xpipe";

    public CheckWrongSentinels4BackupDc(SentinelHelloCollectContext context, Set<HostPort> allSentinels) {
        super(context);
        this.allSentinels = allSentinels;
    }


    @Override
    protected void doExecute() throws Throwable {
        Map<String, Set<SentinelHello>> wrongHellos = checkWrongHellos(context.getSentinelMonitorName(), context.getSentinels(), context.getProcessedHellos());
        if (wrongHellos.values().stream().allMatch(Set::isEmpty)) {
            future().setSuccess();
        } else {
            logger.warn("[{}-{}+{}+{}] {} to find wrong sentinel : {}", LOG_TITLE, context.getInfo().getClusterId(), context.getInfo().getShardId(), context.getInfo().getDcId(), context.getSentinelMonitorName(), wrongHellos.values());
            CatEventMonitor.DEFAULT.logEvent("sentinel.check.wrong.external",
                    String.format("%s-%s-%s-%s:%s", context.getInfo().getClusterId(), context.getInfo().getShardId(), context.getInfo().getDcId(), context.getSentinelMonitorName(), wrongHellos.get(WRONG_OUT_OF_XPIPE)));
            CatEventMonitor.DEFAULT.logEvent("sentinel.check.wrong.internal",
                    String.format("%s-%s-%s-%s:%s", context.getInfo().getClusterId(), context.getInfo().getShardId(), context.getInfo().getDcId(), context.getSentinelMonitorName(), wrongHellos.get(WRONG_OTHER_SHARD)));

        }
    }

    protected Map<String, Set<SentinelHello>> checkWrongHellos(String sentinelMonitorName, Set<HostPort> sentinels, Set<SentinelHello> hellos) {
        Map<String, Set<SentinelHello>> wrongSentinels = new HashMap<>();
        wrongSentinels.put(WRONG_OTHER_SHARD, new HashSet<>());
        wrongSentinels.put(WRONG_OUT_OF_XPIPE, new HashSet<>());

        Iterator<SentinelHello> iterator = hellos.iterator();
        while (iterator.hasNext()) {
            SentinelHello hello = iterator.next();
            if (!hello.getMonitorName().equals(sentinelMonitorName) || !sentinels.contains(hello.getSentinelAddr())) {
                if (allSentinels.contains(hello.getSentinelAddr())) {
                    wrongSentinels.get(WRONG_OTHER_SHARD).add(hello);
                } else {
                    wrongSentinels.get(WRONG_OUT_OF_XPIPE).add(hello);
                }
                iterator.remove();
            }
        }

        return wrongSentinels;
    }

}
