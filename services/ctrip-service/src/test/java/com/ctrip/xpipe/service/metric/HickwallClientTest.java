package com.ctrip.xpipe.service.metric;

import com.google.common.collect.Lists;
import org.junit.Ignore;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.util.ArrayList;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

/**
 * @author chen.zhu
 * <p>
 * Jun 11, 2020
 */
public class HickwallClientTest {

    private Logger logger = LoggerFactory.getLogger(HickwallClientTest.class);
    //manually test
    @Ignore
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

//    @Test
//    public void testCloseIfNotValidConnect() throws IOException {
//        HickwallClient hickwallClient = new HickwallClient("10.5.118.70:7576,10.5.118.69:7576");
//        hickwallClient.closeIfNotValidConnect(null);
//    }
//
//    @Test
//    public void testCloseIfNotValidConnectWithoutClose() throws IOException {
//        HickwallClient hickwallClient = new HickwallClient("10.5.118.70:7576,10.5.118.69:7576");
//        HttpURLConnection httpURLConnection = mock(HttpURLConnection.class);
//        when(httpURLConnection.getErrorStream()).thenReturn(null);
//        hickwallClient.closeIfNotValidConnect(httpURLConnection);
//    }
//
//    @Test
//    public void testCloseIfNotValidConnectWithClose() throws IOException {
//        HickwallClient hickwallClient = new HickwallClient("10.5.118.70:7576,10.5.118.69:7576");
//        HttpURLConnection httpURLConnection = mock(HttpURLConnection.class);
//        InputStream inputStream = mock(InputStream.class);
//        when(httpURLConnection.getErrorStream()).thenReturn(inputStream);
//        doNothing().when(inputStream).close();
//        doNothing().when(httpURLConnection).disconnect();
//        when(httpURLConnection.getErrorStream()).thenReturn(inputStream);
//        hickwallClient.closeIfNotValidConnect(httpURLConnection);
//        verify(inputStream, atLeast(1)).close();
//        verify(httpURLConnection, atLeast(1)).disconnect();
//    }

    private static final int MAX_ARRAY_SIZE = Integer.MAX_VALUE - 8;

    @Test
    public void testOOM() {
        int size = Integer.MAX_VALUE + 8;
        logger.info("[num] {}", size);
        logger.info("[num] {}", size - MAX_ARRAY_SIZE);
        logger.info("[gt 0] {}", ((size - MAX_ARRAY_SIZE) > 0));
    }

    @Ignore
    @Test
    public void testOOM2() {
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        byte[] bytes = new byte[(Integer.MAX_VALUE >> 1)  + 10];
        outputStream.write(bytes, 0, bytes.length);
        outputStream.write(bytes, 0, bytes.length);
    }

}