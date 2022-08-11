package com.ctrip.xpipe.metric;

import com.ctrip.xpipe.metric.MetricData;
import com.ctrip.xpipe.metric.MetricProxy;
import com.ctrip.xpipe.metric.MetricProxyException;

import java.util.Queue;
import java.util.concurrent.LinkedBlockingDeque;

/**
 * @author lishanglin
 * date 2022/7/19
 */
public class MockMetricProxy implements MetricProxy {

    private Queue<MetricData> dataQueue;

    public MockMetricProxy() {
        this.dataQueue = new LinkedBlockingDeque<>();
    }

    @Override
    public void writeBinMultiDataPoint(MetricData data) throws MetricProxyException {
        this.dataQueue.add(data);
    }

    public void reset() {
        this.dataQueue.clear();
    }

    public MetricData poll() {
        return this.dataQueue.poll();
    }

    @Override
    public int getOrder() {
        return 0;
    }
}
