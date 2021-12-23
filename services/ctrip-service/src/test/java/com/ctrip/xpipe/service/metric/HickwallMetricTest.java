package com.ctrip.xpipe.service.metric;

import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.metric.MetricData;
import com.ctrip.xpipe.service.AbstractServiceTest;
import com.ctrip.xpipe.tuple.Pair;
import org.influxdb.dto.Point;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.runners.MockitoJUnitRunner;

import java.lang.reflect.Field;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.*;

/**
 * @author lishanglin
 * date 2021/10/27
 */
@RunWith(MockitoJUnitRunner.class)
public class HickwallMetricTest extends AbstractServiceTest {

    private HickwallMetric hickwallMetric;

    @Mock
    private InfluxDbClient client;

    @Mock
    private HickwallConfig config;

    private int sendInterval = 10;

    private int sendBatch = 2;

    private String dc = "jq", cluster = "cluster1", shard = "shard1";

    private AtomicReference<List<Point>> dataRef = new AtomicReference();

    @Before
    public void setupHickwallMetricTest() throws Exception {
        HickwallMetric.HICKWALL_SEND_INTERVAL = sendInterval;

        when(config.getHickwallBatchSize()).thenReturn(sendBatch);
        when(config.getHickwallAddress()).thenReturn("http://10.0.0.1:80");
        when(config.getHickwallDatabase()).thenReturn("test-db");
        when(config.getHickwallQueueSize()).thenReturn(100);
        when(config.getHickwallWriteMonitor()).thenReturn(false);
        doAnswer(invocationOnMock -> {
            dataRef.set(invocationOnMock.getArgumentAt(0, List.class));
            return null;
        }).when(client).send(any());
        doAnswer(invocationOnMock -> {
            dataRef.set(invocationOnMock.getArgumentAt(0, List.class));
            return null;
        }).when(client).sendWithMonitor(any());

        hickwallMetric = new HickwallMetric(config);
        hickwallMetric.setInfluxDbClient(client);
    }

    @Test
    public void testMetricDelay() throws Exception {
        hickwallMetric.writeBinMultiDataPoint(mockDelayMetricData(100));
        hickwallMetric.writeBinMultiDataPoint(mockDelayMetricData(200));
        waitConditionUntilTimeOut(() -> null != dataRef.get());
        Mockito.verify(client).send(any());
        Mockito.verify(client, never()).sendWithMonitor(any());

        List<Point> sendData = dataRef.get();
        Assert.assertEquals(2, sendData.size());
        Point point1 = sendData.get(0);
        Point point2 = sendData.get(1);

        Assert.assertEquals(new Pair<>("fx.xpipe.delay", 100D), parsePoint(point1));
        Assert.assertEquals(new Pair<>("fx.xpipe.delay", 200D), parsePoint(point2));
    }

    @Test
    public void testSendWithMonitor() throws Exception {
        when(config.getHickwallWriteMonitor()).thenReturn(true);
        hickwallMetric.writeBinMultiDataPoint(mockDelayMetricData(100));
        hickwallMetric.writeBinMultiDataPoint(mockDelayMetricData(200));
        waitConditionUntilTimeOut(() -> null != dataRef.get());
        Mockito.verify(client, never()).send(any());
        Mockito.verify(client).sendWithMonitor(any());
    }

    private Pair<String, Double> parsePoint(Point point) throws Exception {
        Field measurementField = Point.class.getDeclaredField("measurement");
        Field fieldsField = Point.class.getDeclaredField("fields");

        measurementField.setAccessible(true);
        fieldsField.setAccessible(true);

        String measurement = (String)measurementField.get(point);
        Map<String, Object> fields = (Map<String, Object>) fieldsField.get(point);

        return new Pair<>(measurement, (Double) fields.get("value"));
    }

    private MetricData mockDelayMetricData(double delay) {
        MetricData metricData = new MetricData("delay", dc, cluster, shard);
        metricData.setHostPort(new HostPort("127.0.0.1", 6379));
        metricData.setValue(delay);
        metricData.setTimestampMilli(System.currentTimeMillis());
        return metricData;
    }

}
