package com.ctrip.xpipe.service.metric;

import com.ctrip.framework.foundation.Foundation;
import com.ctrip.xpipe.api.foundation.FoundationService;
import com.ctrip.xpipe.concurrent.AbstractExceptionLogTask;
import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.metric.MetricData;
import com.ctrip.xpipe.metric.MetricProxy;
import com.ctrip.xpipe.utils.VisibleForTesting;
import com.ctrip.xpipe.utils.XpipeThreadFactory;
import org.influxdb.InfluxDBIOException;
import org.influxdb.dto.Point;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Map;
import java.util.concurrent.*;

/**
 * @author shyin
 *
 * Jan 6, 2017
 */
public class HickwallMetric implements MetricProxy {

    private HickwallConfig config;

    private BlockingQueue<Point> datas;

    private InfluxDbClient client;

    private ArrayList<Point> dataToSend = null;

    private String srcConsoleIpTag = getFormattedSrcAddr(getLocalIP());

    private String localIp = getLocalIP();

    protected static int HICKWALL_SEND_INTERVAL = 2000;

    private static final int WAIT_SECONDS_AFTER_METRIC_FAIL = 5;

    private static final String CURRENT_DC_ID = FoundationService.DEFAULT.getDataCenter();

    private static final Logger logger = LoggerFactory.getLogger(HickwallMetric.class);

    public HickwallMetric(HickwallConfig config) {
        this.config = config;
        start();
    }

    public HickwallMetric() {
        this(new HickwallConfig());
    }

    private void start() {
        logger.info("Hickwall proxy started.");

        datas = new ArrayBlockingQueue<>(config.getHickwallQueueSize());

        ScheduledExecutorService scheduled = Executors.newScheduledThreadPool(1,
                XpipeThreadFactory.create("HickwallSender", true));
        this.client = new InfluxDbClient(config.getHickwallAddress(), config.getHickwallDatabase());

        scheduled.scheduleWithFixedDelay(new AbstractExceptionLogTask() {
            @Override
            protected void doRun() {

                while(datas.size() >= config.getHickwallBatchSize()) {
                    if (dataToSend == null) {
                        dataToSend = new ArrayList<>();
                        datas.drainTo(dataToSend, config.getHickwallBatchSize());
                    }

                    try {
                        if (config.getHickwallWriteMonitor()) client.sendWithMonitor(dataToSend);
                        else client.send(dataToSend);
                        dataToSend = null;
                    } catch (IOException | InfluxDBIOException ioException) {
                        logger.error("[metric][fail][io] client:{}, msg:{}", client, ioException.getMessage());
                        tryUntilConnected();
                    } catch (Throwable th) {
                        logger.error("[metric][fail][unknown] client:{}", client, th);
                        sleepAfterFail();
                    }
                }
            }
        }, HICKWALL_SEND_INTERVAL, HICKWALL_SEND_INTERVAL, TimeUnit.MILLISECONDS);
    }

    private void tryUntilConnected() {
        while(! Thread.currentThread().isInterrupted()) {
            String address = config.getHickwallAddress();
            String database = config.getHickwallDatabase();
            if (null == client) {
                client = new InfluxDbClient(address, database);
            }

            if (!address.equals(client.getHost()) || !database.equals(client.getDb())) {
                logger.info("[tryUntilConnected][client-update] old: {}", client);
                try {
                    client.close();
                } catch (Throwable th) {
                    logger.warn("[refreshClient][{}] old client close fail", client, th);
                }
                client = new InfluxDbClient(address, database);
                logger.info("[tryUntilConnected][client-update] new: {}", client);
            }

            try {
                logger.info("[tryUntilConnected][begin] {}", client);
                client.ping();
                logger.info("[tryUntilConnected][end] {}", client);
                break;
            } catch (InfluxDBIOException e) {
                logger.error("[tryUntilConnected][fail][{}] sleep a while", address, e);
                sleepAfterFail();
            }
        }
    }

    private void sleepAfterFail() {
        try {
            TimeUnit.SECONDS.sleep(WAIT_SECONDS_AFTER_METRIC_FAIL);
        } catch (InterruptedException interrupt) {
            Thread.currentThread().interrupt();
        }
    }

    @Override
    public void writeBinMultiDataPoint(MetricData rawData) {
        Point point = metricData2InfluxDbPoint(rawData);

        if (!datas.offer(point)) {
            logger.warn("[writeBinMultiDataPoint][queue-overflow] drop data {}", rawData);
        }
    }

    private Point metricData2InfluxDbPoint(MetricData md) {
        Point.Builder pointBuilder = Point.measurement(String.format("fx.xpipe.%s", md.getMetricType()))
                .time(md.getTimestampMilli(), TimeUnit.MILLISECONDS)
                .addField("value", md.getValue())
                .tag("srcaddr", localIp)
                .tag("app", "fx")
                .tag("source", CURRENT_DC_ID);

        if (null != md.getDcName()) pointBuilder.tag("dc", md.getDcName());
        if (null != md.getHostPort()) pointBuilder.tag("address", md.getHostPort().toString());
        if (null != md.getClusterName()) pointBuilder.tag("cluster", md.getClusterName());
        if (null != md.getShardName()) pointBuilder.tag("shard", md.getShardName());
        if (null != md.getClusterType()) pointBuilder.tag("clustertype", md.getClusterType());
        if (null != md.getTags() && !md.getTags().isEmpty()) {
            for(Map.Entry<String, String> entry : md.getTags().entrySet()) {
                pointBuilder.tag(entry.getKey(), entry.getValue());
            }
        }

        return pointBuilder.build();
    }

    private String getLocalIP() {
        return Foundation.net().getHostAddress();
    }

    @VisibleForTesting
    protected String getFormattedRedisAddr(HostPort hostPort) {
        return hostPort.getHost().replaceAll("\\.", "_") + "_" + hostPort.getPort();
    }

    @VisibleForTesting
    protected String getFormattedSrcAddr(String ipAddr) {
        return ipAddr.replaceAll("\\.", "_");
    }

    @VisibleForTesting
    protected void setInfluxDbClient(InfluxDbClient client) {
        if (null != this.client) {
            try {
                this.client.close();
            } catch (Throwable th) {
                logger.info("[setInfluxDbClient][close fail] old client {}", this.client);
            }
        }
        this.client = client;
    }

    @Override
    public int getOrder() {
        return 0;
    }

}
