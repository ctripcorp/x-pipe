package com.ctrip.xpipe.redis.console.service.impl;

import com.ctrip.xpipe.redis.console.AbstractConsoleIntegrationTest;
import com.ctrip.xpipe.redis.console.model.DcTbl;
import com.ctrip.xpipe.redis.console.model.consoleportal.DcListDcModel;
import com.ctrip.xpipe.redis.console.service.meta.impl.AdvancedDcMetaService;
import com.ctrip.xpipe.redis.core.entity.*;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import java.util.LinkedList;
import java.util.List;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

/**
 * @author tt.tu
 * Oct 8, 2018
 */
public class DcServiceImplTest extends AbstractConsoleIntegrationTest{

    @InjectMocks
    private DcServiceImpl dcService = new DcServiceImpl();

    @Mock
    private AdvancedDcMetaService dcMetaService;

    @Before
    public void beforeDcServiceImplTest(){
        MockitoAnnotations.initMocks(this);
        when(dcMetaService.getDcMeta(any())).thenReturn(toBuild().findDc("jq"));
    }

    private XpipeMeta toBuild(){
        XpipeMeta xpipeMeta = new XpipeMeta();

        DcMeta activeDc = new DcMeta("jq");
        DcMeta backupDc = new DcMeta("oy");

        String cluster1 = "cluster1";
        ClusterMeta clusterMeta = new ClusterMeta(cluster1);
        clusterMeta.setParent(activeDc);
        activeDc.addCluster(clusterMeta);
        clusterMeta.setActiveDc("jq");
        clusterMeta.setBackupDcs("oy");

        String cluster2 = "cluster2";
        ClusterMeta clusterMeta2 = new ClusterMeta(cluster2);
        clusterMeta2.setParent(activeDc);
        activeDc.addCluster(clusterMeta2);
        clusterMeta2.setActiveDc("jq");
        clusterMeta2.setBackupDcs("oy");

        String cluster12 = "cluster1";
        ClusterMeta clusterMeta12 = new ClusterMeta(cluster12);
        clusterMeta12.setParent(backupDc);
        backupDc.addCluster(clusterMeta12);
        clusterMeta12.setActiveDc("jq");
        clusterMeta12.setBackupDcs("oy");

        String cluster22 = "cluster2";
        ClusterMeta clusterMeta22 = new ClusterMeta(cluster22);
        clusterMeta22.setParent(backupDc);
        backupDc.addCluster(clusterMeta22);
        clusterMeta22.setActiveDc("jq");
        clusterMeta22.setBackupDcs("oy");

        ShardMeta shardMeta1 = new ShardMeta("shard1");
        shardMeta1.setParent(clusterMeta);
        clusterMeta.addShard(shardMeta1);
        shardMeta1.addRedis(new RedisMeta().setIp("127.0.0.1").setPort(6379));
        shardMeta1.addRedis(new RedisMeta().setIp("127.0.0.2").setPort(6379));

        ShardMeta shardMeta2 = new ShardMeta("shard2");
        shardMeta2.setParent(clusterMeta12);
        clusterMeta12.addShard(shardMeta2);
        shardMeta2.addRedis(new RedisMeta().setIp("127.0.0.1").setPort(6380));
        shardMeta2.addRedis(new RedisMeta().setIp("127.0.0.2").setPort(6380));

        ShardMeta shardMeta12 = new ShardMeta("shard1");
        shardMeta12.setParent(clusterMeta12);
        clusterMeta12.addShard(shardMeta12);
        shardMeta12.addRedis(new RedisMeta().setIp("127.0.0.3").setPort(6379));
        shardMeta12.addRedis(new RedisMeta().setIp("127.0.0.4").setPort(6379));

        ShardMeta shardMeta22 = new ShardMeta("shard2");
        shardMeta22.setParent(clusterMeta22);
        clusterMeta22.addShard(shardMeta22);
        shardMeta22.addRedis(new RedisMeta().setIp("127.0.0.3").setPort(6380));
        shardMeta22.addRedis(new RedisMeta().setIp("127.0.0.4").setPort(6380));

        xpipeMeta.addDc(activeDc).addDc(backupDc);
        return xpipeMeta;
    }

    private List<DcTbl> toBuildTbl(){
        List<DcTbl> result = new LinkedList<>();

        DcTbl tbl1 = new DcTbl();
        tbl1.setDcName("jq").setId(1);

        DcTbl tbl2 = new DcTbl();
        tbl2.setDcName("oy").setId(2);

        result.add(tbl1);
        result.add(tbl2);

        return result;
    }

    @Test
    public void testFindAllDcsRichinfo(){
        dcService = spy(dcService);
        Mockito.doReturn(toBuildTbl()).when(dcService).findAllDcs();
        List<DcListDcModel> result = dcService.findAllDcsRichInfo();
        Assert.assertEquals(2, result.size());
        result.forEach(dcListDcModel -> {
            if (dcListDcModel.getDcName() == "jq"){
                Assert.assertEquals(2, (long)dcListDcModel.getRedisCount());
                Assert.assertEquals(2, (long)dcListDcModel.getClusterCount());
            }else if (dcListDcModel.getDcName() == "oy"){
                Assert.assertEquals(2, (long)dcListDcModel.getRedisCount());
                Assert.assertEquals(2, (long)dcListDcModel.getClusterCount());
            }

        });
    }
}
