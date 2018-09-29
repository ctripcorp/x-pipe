package com.ctrip.xpipe.redis.meta.server.meta.impl;

import com.ctrip.xpipe.api.monitor.EventMonitor;
import com.ctrip.xpipe.redis.core.entity.ClusterMeta;
import com.ctrip.xpipe.redis.core.entity.DcMeta;
import com.ctrip.xpipe.redis.core.entity.ShardMeta;
import com.ctrip.xpipe.redis.core.entity.XpipeMeta;
import com.ctrip.xpipe.redis.core.meta.MetaClone;
import com.ctrip.xpipe.redis.meta.server.AbstractMetaServerTest;
import org.junit.Before;
import org.junit.Test;

/**
 * @author wenchao.meng
 *         <p>
 *         Nov 13, 2017
 */
public class DefaultDcMetaCacheTest extends AbstractMetaServerTest{

    private DefaultDcMetaCache dcMetaCache;

    @Before
    public void beforeDefaultDcMetaCacheTest(){
        dcMetaCache = new DefaultDcMetaCache();
    }

    @Test
    public void testChangeDcMetaLog(){

        //just check exception

        EventMonitor.DEFAULT.logEvent("type", getTestName());

        XpipeMeta xpipeMeta = getXpipeMeta();
        DcMeta dcMeta = (DcMeta) xpipeMeta.getDcs().values().toArray()[0];

        DcMeta future = MetaClone.clone(dcMeta);
        ClusterMeta futureCluster = (ClusterMeta) future.getClusters().values().toArray()[0];
        futureCluster.addShard(new ShardMeta().setId(randomString(5)));

        future.addCluster(new ClusterMeta().setId(randomString(10)));

        dcMetaCache.changeDcMeta(dcMeta, future);

        dcMetaCache.clusterAdded(new ClusterMeta().setId("add_" + randomString(5)));
        dcMetaCache.clusterDeleted("del_" + randomString(5));

    }
}
