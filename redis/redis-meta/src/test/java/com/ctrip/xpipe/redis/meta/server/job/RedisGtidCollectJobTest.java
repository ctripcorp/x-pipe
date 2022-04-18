package com.ctrip.xpipe.redis.meta.server.job;

import com.ctrip.xpipe.api.command.CommandFuture;
import com.ctrip.xpipe.cluster.ClusterType;
import com.ctrip.xpipe.redis.core.entity.RedisMeta;
import com.ctrip.xpipe.redis.meta.server.AbstractMetaServerTest;
import com.ctrip.xpipe.redis.meta.server.meta.DcMetaCache;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.List;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

/**
 * @author ayq
 * <p>
 * 2022/4/18 00:13
 */
@RunWith(MockitoJUnitRunner.class)
public class RedisGtidCollectJobTest extends AbstractMetaServerTest {

    private static final String sid0 = "a1";
    private static final String sid1 = "b1";
    private static final String gtid0 = "a1:1-10:15-20";
    private static final String gtid1 = "b1:1-8";

    private RedisGtidCollectJob job;
    private long clusterDbId = 1L;
    private long shardDbId = 1L;

    @Mock
    private DcMetaCache dcMetaCache;

    @Before
    public void beforeRedisGtidCollectJobTest() throws Exception {

        when(dcMetaCache.getClusterType(clusterDbId)).thenReturn(ClusterType.HETERO);

        job = new RedisGtidCollectJob(clusterDbId, shardDbId, dcMetaCache, scheduled,
                getXpipeNettyClientKeyedObjectPool());
    }

    @Test
    public void testCollectGtidAndSids() throws Exception {

        List<RedisMeta> redises = createRandomRedises(2);
        when(dcMetaCache.getShardRedises(clusterDbId, shardDbId)).thenReturn(redises);
        startServer(redises.get(0).getPort(), String.format("+%s\r\n", gtid0));
        startServer(redises.get(1).getPort(), String.format("+%s\r\n", gtid1));

        CommandFuture future = job.execute();
        Thread.sleep(100);
        //TODO ayq why future isSuccess false
//        while (!future.isSuccess()) {
//            logger.info("future is not done, wait another 10ms");
//            Thread.sleep(10);
//        }

        assertEquals(gtid0, redises.get(0).getGtid());
        assertEquals(gtid1, redises.get(1).getGtid());
        assertEquals(sid0, redises.get(0).getSid());
        assertEquals(sid1, redises.get(1).getSid());
    }
}