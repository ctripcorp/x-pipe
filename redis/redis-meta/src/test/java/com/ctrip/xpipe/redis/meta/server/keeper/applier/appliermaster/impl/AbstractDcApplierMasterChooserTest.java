package com.ctrip.xpipe.redis.meta.server.keeper.applier.appliermaster.impl;

import com.ctrip.xpipe.redis.meta.server.AbstractMetaServerTest;
import com.ctrip.xpipe.redis.meta.server.meta.CurrentMetaManager;
import com.ctrip.xpipe.redis.meta.server.meta.DcMetaCache;
import com.ctrip.xpipe.redis.meta.server.multidc.MultiDcService;
import org.junit.Before;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

/**
 * @author ayq
 * <p>
 * 2022/4/17 10:42
 */
@RunWith(MockitoJUnitRunner.class)
public class AbstractDcApplierMasterChooserTest extends AbstractMetaServerTest {

    protected int checkIntervalSeconds = 1;

    protected String primaryDc = "jq";

    protected String clusterId = "cluster1";

    protected String shardId = "shard1";

    protected Long clusterDbId = 1L;

    protected Long clusterHeteroId = 7L;

    protected Long shardDbId = 1L;

    @Mock
    protected DcMetaCache dcMetaCache;

    @Mock
    protected CurrentMetaManager currentMetaManager;

    @Mock
    protected MultiDcService multiDcService;

    @Before
    public void beforeAbstractDcApplierMasterChooserTest(){

    }
}
