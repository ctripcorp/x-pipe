package com.ctrip.xpipe.redis.meta.server.keeper.applier.appliermaster.impl;

import com.ctrip.xpipe.redis.core.entity.ApplierMeta;
import com.ctrip.xpipe.redis.core.entity.KeeperMeta;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;

import static org.mockito.Mockito.when;

/**
 * @author ayq
 * <p>
 * 2022/4/17 10:38
 */
public class DefaultApplierMasterChooserManagerTest extends AbstractDcApplierMasterChooserTest {

    private DefaultDcApplierMasterChooser defaultDcApplierMasterChooser;

    @Before
    public void beforeDefaultDcApplierMasterChooserTest() throws Exception{

        defaultDcApplierMasterChooser = new DefaultDcApplierMasterChooser(clusterDbId, shardDbId,
                multiDcService, dcMetaCache, currentMetaManager, scheduled);
    }

    @Test
    public void testMasterChooserAlgorithm(){

        Assert.assertNull(defaultDcApplierMasterChooser.getApplierMasterChooserAlgorithm());

        when(dcMetaCache.getShardAppliers(clusterDbId, shardDbId)).thenReturn(Arrays.asList(new ApplierMeta()));
        when(dcMetaCache.getShardKeepers(clusterDbId, shardDbId)).thenReturn(null);

        defaultDcApplierMasterChooser.chooseApplierMaster();

        Assert.assertTrue(defaultDcApplierMasterChooser.getApplierMasterChooserAlgorithm() instanceof ShardWithoutKeeperApplierMasterChooserAlgorithm);


        when(dcMetaCache.getShardAppliers(clusterDbId, shardDbId)).thenReturn(Arrays.asList(new ApplierMeta()));
        when(dcMetaCache.getShardKeepers(clusterDbId, shardDbId)).thenReturn(Arrays.asList(new KeeperMeta()));

        defaultDcApplierMasterChooser.chooseApplierMaster();

        Assert.assertTrue(defaultDcApplierMasterChooser.getApplierMasterChooserAlgorithm() instanceof ShardWithKeeperApplierMasterChooserAlgorithm);
    }
}