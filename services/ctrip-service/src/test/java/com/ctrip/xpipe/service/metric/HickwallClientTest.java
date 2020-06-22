package com.ctrip.xpipe.service.metric;

import com.google.common.collect.Lists;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.*;

/**
 * @author chen.zhu
 * <p>
 * Jun 11, 2020
 */
public class HickwallClientTest {

    @Test
    public void testSend() throws IOException, InterruptedException {
        HickwallClient hickwallClient = new HickwallClient("10.5.118.70:7576,10.5.118.69:7576");
        while(true) {
            DataPoint point = new DataPoint("fx.xpipe.delay.cluster.shard", 1000.0d, System.currentTimeMillis());
            point.setEndpoint("cluster.shard.10_5_108_201_6379.127_0_0_1");
            point.getMeta().put("measurement", "fx.xpipe.delay");
            point.getTag().put("cluster", "cluster");
            point.getTag().put("shard", "shard");
            point.getTag().put("address", "10.5.108.201:6379");
            point.getTag().put("srcaddr", "127.0.0.1");
            point.getTag().put("app", "fx");
            point.getTag().put("dc", "LOCAL");
            hickwallClient.send(Lists.newArrayList(point));
            Thread.sleep(1000);
        }
    }
}