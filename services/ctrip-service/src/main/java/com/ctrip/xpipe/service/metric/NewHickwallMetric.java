package com.ctrip.xpipe.service.metric;

import com.ctrip.framework.foundation.Foundation;
import com.ctrip.ops.hickwall.HickwallUDPReporter;
import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.metric.MetricData;
import com.ctrip.xpipe.metric.MetricProxy;
import com.ctrip.xpipe.metric.MetricProxyException;
import com.google.common.collect.Maps;
import io.dropwizard.metrics5.Histogram;
import io.dropwizard.metrics5.MetricName;
import io.dropwizard.metrics5.MetricRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.SortedMap;
import java.util.concurrent.TimeUnit;

/**
 * @author zhuchen
 * Apr 2020/4/9 2020
 */
public class NewHickwallMetric implements MetricProxy {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    private final HickwallConfig config = new HickwallConfig();

    private final MetricRegistry metrics = new MetricRegistry();

    private boolean isEnable = false;

    private String localIp = getLocalIP();

    private void setHickwallEnable() {
        try {
            if (!isEnable) {
                String hickwallDomain = config.getHickwallHostPort();
                HostPort hostPort = HostPort.fromString(hickwallDomain);
                HickwallUDPReporter.enable(
                        metrics,
                        30,
                        TimeUnit.SECONDS,
                        hostPort.getHost(),
                        hostPort.getPort(),
                        "FX"
                );
                isEnable = true;
            }
        } catch (Exception e) {
            logger.warn("setHickwallEnable exception: ", e);
        }
    }

    public void reportData(MetricData data) {

        setHickwallEnable();

        Map<String, String> tags = data.getTags() != null ?  data.getTags() : Maps.newHashMap();
        tags.put("cluster", data.getClusterName());
        tags.put("shard", data.getShardName());
        tags.put("address", data.getHostPort().toString());
        tags.put("srcaddr", localIp);
        tags.put("app", "fx");
        tags.put("dc", data.getDcName());

        MetricName metricName = MetricName.build(data.getMetricType()).tagged(tags);
        Histogram histogram = metrics.histogram(metricName);
        histogram.update((long) data.getValue());
    }

    @Override
    public void writeBinMultiDataPoint(MetricData data) throws MetricProxyException {
        reportData(data);
    }

    @Override
    public int getOrder() {
        return 0;
    }

    private String getLocalIP() {
        return Foundation.net().getHostAddress();
    }
}
