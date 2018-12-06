package com.ctrip.xpipe.redis.console.proxy;

import com.ctrip.xpipe.AbstractTest;
import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.metric.MetricData;
import com.ctrip.xpipe.metric.MetricProxy;
import com.ctrip.xpipe.metric.MetricProxyException;
import com.ctrip.xpipe.redis.core.proxy.monitor.PingStatsResult;
import com.google.common.collect.Lists;
import org.junit.Assert;
import org.junit.Test;
import org.springframework.core.annotation.Order;

import java.util.List;

import static org.junit.Assert.*;

public class ProxyPingRecorderTest extends AbstractTest {

    private List<MetricData> datas = Lists.newArrayList();

    private ProxyPingRecorder recorder = new ProxyPingRecorder(){
        @Override
        public MetricProxy getMetricProxy() {
            return new FakeMetricProxy();
        }
    };

    @Test
    public void ackPingStatsResult() {
        recorder.ackPingStatsResult(getRealTimeResults());
        Assert.assertNotNull(datas);
    }

    private List<PingStatsResult> getRealTimeResults() {
        return Lists.newArrayList(getPingStatsResult(), getPingStatsResult(), getPingStatsResult());
    }

    private PingStatsResult getPingStatsResult() {
        long start = System.currentTimeMillis();
        return new PingStatsResult(start, start + 200, new HostPort("127.0.0.1", 8080), localHostport(randomPort()));
    }

    private class FakeMetricProxy implements MetricProxy {

        @Override
        public void writeBinMultiDataPoint(MetricData data) throws MetricProxyException {
            datas.add(data);
        }

        @Override
        public int getOrder() {
            return 0;
        }
    }
}