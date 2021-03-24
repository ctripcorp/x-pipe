package com.ctrip.xpipe.redis.console.service.meta.impl;

import com.ctrip.xpipe.redis.console.AbstractConsoleIntegrationTest;
import com.ctrip.xpipe.redis.console.model.ClusterTbl;
import com.ctrip.xpipe.redis.console.model.DcTbl;
import com.ctrip.xpipe.redis.console.service.ClusterService;
import com.ctrip.xpipe.redis.console.service.DcService;
import com.ctrip.xpipe.redis.console.service.migration.impl.MigrationServiceImpl;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;

import java.io.IOException;

/**
 * @author lishanglin
 * date 2021/3/23
 */
public class ClusterMetaServiceMigrationStatusChangeTest extends AbstractConsoleIntegrationTest {

    @Autowired
    private ClusterMetaServiceImpl  clusterMetaServiceImpl;

    @Autowired
    private MigrationServiceImpl migrationService;

    @Autowired
    private ClusterService clusterService;

    @Autowired
    private DcService dcService;

    @Before
    public void prepare() {
        clusterMetaServiceImpl.setMigrationService(migrationService);
    }

    @Test
    public void testMigrationStatusChangeOnGetCurrentPrimaryDc() {
        ClusterTbl clusterTbl = clusterService.find("cluster1");
        DcTbl jqDcTbl = dcService.findByDcName("jq");
        DcTbl oyDcTbl = dcService.findByDcName("oy");
        DcTbl rbDcTbl = new DcTbl().setDcName("rb");
        Assert.assertEquals(jqDcTbl.getId(), clusterMetaServiceImpl.getClusterMetaCurrentPrimaryDc(jqDcTbl, clusterTbl));
        Assert.assertEquals(oyDcTbl.getId(), clusterMetaServiceImpl.getClusterMetaCurrentPrimaryDc(oyDcTbl, clusterTbl));
        Assert.assertEquals(jqDcTbl.getId(), clusterMetaServiceImpl.getClusterMetaCurrentPrimaryDc(rbDcTbl, clusterTbl));
    }


    @Override
    protected String prepareDatas() throws IOException {
        return "insert into CLUSTER_TBL (id,cluster_name,activedc_id,cluster_description,cluster_last_modified_time,status,migration_event_id,is_xpipe_interested) values (1,'cluster1',1,'Cluster:cluster1 , ActiveDC : A','0000000000000000','Migrating',1,1);\n" +
                "insert into MIGRATION_EVENT_TBL (id,event_tag) values (1,'cluster1-111');\n" +
                "insert into MIGRATION_CLUSTER_TBL (id,migration_event_id,cluster_id,source_dc_id, destination_dc_id,status) values (1,1,1,1,2,'Success');";
    }

}
