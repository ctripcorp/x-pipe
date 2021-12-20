package com.ctrip.xpipe.redis.meta.server.dcchange.impl;

import com.ctrip.xpipe.redis.core.entity.RedisMeta;
import com.ctrip.xpipe.redis.meta.server.AbstractMetaServerTest;
import com.ctrip.xpipe.redis.meta.server.dcchange.NewMasterChooser;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.runners.MockitoJUnitRunner;

import java.util.Arrays;
import java.util.List;

/**
 * @author lishanglin
 * date 2021/6/4
 */
@RunWith(MockitoJUnitRunner.class)
public class ClusterShardCachedNewMasterChooserTest extends AbstractMetaServerTest {

    @Mock
    private NewMasterChooser innerChooser;

    private ClusterShardCachedNewMasterChooser chooser;

    private long timeoutMilli = 200L;

    private String cluster = "cluster";
    private String shard = "shard";
    private Long clusterDbId = 1L;
    private Long shardDbId = 1L;

    @Before
    public void setupClusterShardCachedNewMasterChooserTest() {
        Mockito.doAnswer(invocation -> {
            List<RedisMeta> redises = invocation.getArgumentAt(0, List.class);
            return redises.get(0);
        }).when(innerChooser).choose(Mockito.anyList());

        ClusterShardCachedNewMasterChooser.clear();
        chooser = ClusterShardCachedNewMasterChooser.wrapChooser(clusterDbId, shardDbId, innerChooser, this::getTimeoutMilli, scheduled);
    }

    @Test
    public void testResultCacheAndExpire() {
        List<RedisMeta> redises = Arrays.asList(mockRedisMeta("10.0.0.1", 6379), mockRedisMeta("10.0.0.2", 6379));
        RedisMeta newMaster = chooser.choose(redises);
        Assert.assertEquals(newMaster, chooser.choose(redises));
        Mockito.verify(innerChooser, Mockito.times(1)).choose(Mockito.anyList());

        sleep((int) timeoutMilli);
        Assert.assertEquals(newMaster, chooser.choose(redises));
        Mockito.verify(innerChooser, Mockito.times(2)).choose(Mockito.anyList());
    }

    @Test
    public void testRedisMetaChange() {
        List<RedisMeta> redises = Arrays.asList(mockRedisMeta("10.0.0.1", 6379), mockRedisMeta("10.0.0.2", 6379));
        RedisMeta newMaster = chooser.choose(redises);
        redises = Arrays.asList(mockRedisMeta("10.0.0.3", 6379), mockRedisMeta("10.0.0.4", 6379));
        Assert.assertNotEquals(newMaster, chooser.choose(redises));
        Mockito.verify(innerChooser, Mockito.times(2)).choose(Mockito.anyList());
    }

    @Test
    public void testSystemTimeRollback() {
        List<RedisMeta> redises = Arrays.asList(mockRedisMeta("10.0.0.1", 6379), mockRedisMeta("10.0.0.2", 6379));
        RedisMeta newMaster = chooser.choose(redises);
        chooser.setUpdatedAt(System.currentTimeMillis() + 10);
        Assert.assertEquals(newMaster, chooser.choose(redises));
        Mockito.verify(innerChooser, Mockito.times(2)).choose(Mockito.anyList());
    }

    private long getTimeoutMilli() {
        return timeoutMilli;
    }

    private RedisMeta mockRedisMeta(String ip, int port) {
        RedisMeta redisMeta = new RedisMeta();
        redisMeta.setIp(ip);
        redisMeta.setPort(port);
        return redisMeta;
    }

}
