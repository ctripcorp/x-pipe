package com.ctrip.xpipe.redis.proxy.monitor;

import com.ctrip.xpipe.api.monitor.EventMonitor;
import com.ctrip.xpipe.command.AbstractCommand;
import com.ctrip.xpipe.command.ParallelCommandChain;
import com.ctrip.xpipe.redis.proxy.spring.Production;
import com.ctrip.xpipe.utils.OsUtils;
import com.ctrip.xpipe.utils.XpipeThreadFactory;
import io.netty.buffer.PoolArenaMetric;
import io.netty.buffer.PooledByteBufAllocator;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * @author chen.zhu
 * <p>
 * Jul 04, 2018
 */
@Component
public class NettyPoolArenaMetricReporter {

    private ScheduledExecutorService scheduled;

    private static final String NEWLINE = "\r\n";

    public NettyPoolArenaMetricReporter() {
    }

    @PostConstruct
    public void reportPoolArenaMetric() {
        scheduled = new ScheduledThreadPoolExecutor(OsUtils.getCpuCount(), XpipeThreadFactory.create("PoolArenaMetricReporter"));
        scheduled.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                reportJob();
            }
        }, 0, 1, TimeUnit.MINUTES);
    }

    private void reportJob() {
        PooledByteBufAllocator allocator = PooledByteBufAllocator.DEFAULT;
        List<PoolArenaMetric> arenaMetrics = allocator.directArenas();
        ParallelCommandChain chain = new ParallelCommandChain(scheduled);
        int index = 0;
        for(PoolArenaMetric metric : arenaMetrics) {
            chain.add(new ReportArenaMetricCommand(metric, index++));
        }
        chain.execute();
    }

    class ReportArenaMetricCommand extends AbstractCommand {

        private PoolArenaMetric metric;

        private int index;

        public ReportArenaMetricCommand(PoolArenaMetric metric, int index) {
            this.metric = metric;
            this.index = index;
        }

        @Override
        protected void doExecute() throws Exception {
            try {
                reportSumStats();
                reportDetails();
                future().setSuccess();
            } catch (Exception e) {
                future().setFailure(e);
            }
        }

        @Override
        protected void doReset() {

        }

        @Override
        public String getName() {
            return "Report-Arena-Metric";
        }

        private void reportSumStats() {
            EventMonitor.DEFAULT.logEvent("ArenaMetric-" + index, metric.toString());
            logger.info("ArenaMetric-{} Summary: {} {}", index, NEWLINE, metric.toString());
        }

        private void reportDetails() {
            logger.info("ArenaMetric-{} Details: {} {}", index, NEWLINE, getArenaMetricDetail(metric));
            monitorLogArenaMetricDetail(metric, index);
        }
    }

    private static String getArenaMetricDetail(PoolArenaMetric metric) {
        StringBuilder sb = new StringBuilder();
        sb.append("numActiveAllocations: ").append(metric.numActiveAllocations()).append(NEWLINE);
        sb.append("numActiveBytes: ").append(metric.numActiveBytes()).append(NEWLINE);
        sb.append("numActiveHugeAllocations: ").append(metric.numActiveHugeAllocations()).append(NEWLINE);
        sb.append("numActiveNormalAllocations: ").append(metric.numActiveNormalAllocations()).append(NEWLINE);
        sb.append("numActiveSmallAllocations: ").append(metric.numActiveSmallAllocations()).append(NEWLINE);
        sb.append("numActiveTinyAllocations: ").append(metric.numActiveTinyAllocations()).append(NEWLINE);
        sb.append("numAllocations: ").append(metric.numAllocations()).append(NEWLINE);
        sb.append("numDeallocations: ").append(metric.numDeallocations()).append(NEWLINE);
        sb.append("numHugeDeallocations: ").append(metric.numHugeDeallocations()).append(NEWLINE);
        sb.append("numNormalDeallocations: ").append(metric.numNormalDeallocations()).append(NEWLINE);
        sb.append("numSmallDeallocations: ").append(metric.numSmallDeallocations()).append(NEWLINE);
        sb.append("numTinyDeallocations: ").append(metric.numTinyDeallocations()).append(NEWLINE);
        sb.append("numSmallSubpages: ").append(metric.numSmallSubpages()).append(NEWLINE);
        sb.append("numTinySubpages: ").append(metric.numTinySubpages()).append(NEWLINE);
        return sb.toString();
    }

    private static void monitorLogArenaMetricDetail(PoolArenaMetric metric, int index) {
        EventMonitor monitor = EventMonitor.DEFAULT;
        String type = String.format("ArenaMetric-%d", index);
        monitor.logEvent(type, "numActiveAllocations", metric.numActiveAllocations());
        monitor.logEvent(type, "numActiveBytes", metric.numActiveBytes());
        monitor.logEvent(type, "numActiveHugeAllocations", metric.numActiveHugeAllocations());
        monitor.logEvent(type, "numActiveNormalAllocations", metric.numActiveNormalAllocations());
        monitor.logEvent(type, "numActiveSmallAllocations", metric.numActiveSmallAllocations());
        monitor.logEvent(type, "numActiveTinyAllocations", metric.numActiveTinyAllocations());
        monitor.logEvent(type, "numAllocations", metric.numAllocations());
        monitor.logEvent(type, "numDeallocations", metric.numDeallocations());
        monitor.logEvent(type, "numHugeAllocations", metric.numHugeAllocations());
        monitor.logEvent(type, "numHugeDeallocations", metric.numHugeDeallocations());
        monitor.logEvent(type, "numNormalAllocations", metric.numNormalAllocations());
        monitor.logEvent(type, "numNormalDeallocations", metric.numNormalDeallocations());
        monitor.logEvent(type, "numSmallAllocations", metric.numSmallAllocations());
        monitor.logEvent(type, "numSmallDeallocations", metric.numSmallDeallocations());
        monitor.logEvent(type, "numTinyAllocations", metric.numTinyAllocations());
        monitor.logEvent(type, "numTinyDeallocations", metric.numTinyDeallocations());
        monitor.logEvent(type, "numTinySubpages", metric.numTinySubpages());
    }
}
