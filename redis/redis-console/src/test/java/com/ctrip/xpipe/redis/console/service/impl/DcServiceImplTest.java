package com.ctrip.xpipe.redis.console.service.impl;

import com.ctrip.xpipe.cluster.ClusterType;
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

import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

/**
 * @author tt.tu
 * Oct 8, 2018
 */
public class DcServiceImplTest extends AbstractConsoleIntegrationTest {

    @InjectMocks
    private DcServiceImpl dcService = new DcServiceImpl();

    @Mock
    private AdvancedDcMetaService dcMetaService;
    private XpipeMeta xpipeMeta = new XpipeMeta();


    @Before
    public void beforeDcServiceImplTest() {
        MockitoAnnotations.initMocks(this);
        toBuild();
    }

    private XpipeMeta toBuild() {

        DcMeta jqDc = new DcMeta("jq");
        DcMeta oyDc = new DcMeta("oy");
        String singleDcCluster = "singleDcCluster";
        ClusterMeta singleDcClusterMeta = new ClusterMeta(singleDcCluster).setType("single_dc");
        singleDcClusterMeta.setParent(jqDc);
        jqDc.addCluster(singleDcClusterMeta);
        singleDcClusterMeta.setActiveDc("jq");

        ShardMeta singleDcShardMeta = new ShardMeta("singleDcShard");
        singleDcShardMeta.setParent(singleDcClusterMeta);
        singleDcClusterMeta.addShard(singleDcShardMeta);
        singleDcShardMeta.addRedis(new RedisMeta().setIp("127.0.0.1").setPort(7000));
        singleDcShardMeta.addRedis(new RedisMeta().setIp("127.0.0.1").setPort(7001));

        String localDcCluster = "localDcCluster";
        ClusterMeta localDcClusterMetaJq = new ClusterMeta(localDcCluster).setType("local_dc");
        localDcClusterMetaJq.setParent(jqDc);
        jqDc.addCluster(localDcClusterMetaJq);
        localDcClusterMetaJq.setDcs("oy,jq");

        ShardMeta localDcClusterMetaJqShardMeta = new ShardMeta("localDcClusterMetaJqShard");
        localDcClusterMetaJqShardMeta.setParent(localDcClusterMetaJq);
        localDcClusterMetaJq.addShard(localDcClusterMetaJqShardMeta);
        localDcClusterMetaJqShardMeta.addRedis(new RedisMeta().setIp("127.0.0.1").setPort(8000));
        localDcClusterMetaJqShardMeta.addRedis(new RedisMeta().setIp("127.0.0.1").setPort(8001));

        ClusterMeta localDcClusterMetaOy = new ClusterMeta(localDcCluster).setType("local_dc");
        localDcClusterMetaOy.setParent(oyDc);
        oyDc.addCluster(localDcClusterMetaOy);
        localDcClusterMetaOy.setDcs("oy,jq");

        ShardMeta localDcClusterMetaOyShardMeta = new ShardMeta("localDcClusterMetaOyShard");
        localDcClusterMetaOyShardMeta.setParent(localDcClusterMetaOy);
        localDcClusterMetaOy.addShard(localDcClusterMetaOyShardMeta);
        localDcClusterMetaOyShardMeta.addRedis(new RedisMeta().setIp("127.0.0.1").setPort(8002));
        localDcClusterMetaOyShardMeta.addRedis(new RedisMeta().setIp("127.0.0.1").setPort(8003));


        String crossDcCluster = "crossDcCluster";
        ClusterMeta crossDcClusterMetaJq = new ClusterMeta(crossDcCluster).setType("cross_dc");
        crossDcClusterMetaJq.setParent(jqDc);
        jqDc.addCluster(crossDcClusterMetaJq);
        crossDcClusterMetaJq.setDcs("oy,jq");

        ShardMeta crossDcClusterMetaJqShardMeta = new ShardMeta("crossDcClusterMetaJqShard");
        crossDcClusterMetaJqShardMeta.setParent(crossDcClusterMetaJq);
        crossDcClusterMetaJq.addShard(crossDcClusterMetaJqShardMeta);
        crossDcClusterMetaJqShardMeta.addRedis(new RedisMeta().setIp("127.0.0.1").setPort(9000));
        crossDcClusterMetaJqShardMeta.addRedis(new RedisMeta().setIp("127.0.0.1").setPort(9001));

        ClusterMeta crossDcClusterMetaOy = new ClusterMeta(crossDcCluster).setType("cross_dc");
        crossDcClusterMetaOy.setParent(oyDc);
        oyDc.addCluster(crossDcClusterMetaOy);
        crossDcClusterMetaOy.setDcs("oy,jq");

        ShardMeta crossDcClusterMetaOyShardMeta = new ShardMeta("crossDcClusterMetaOyShard");
        crossDcClusterMetaOyShardMeta.setParent(crossDcClusterMetaOy);
        crossDcClusterMetaOy.addShard(crossDcClusterMetaOyShardMeta);
        crossDcClusterMetaOyShardMeta.addRedis(new RedisMeta().setIp("127.0.0.1").setPort(9002));
        crossDcClusterMetaOyShardMeta.addRedis(new RedisMeta().setIp("127.0.0.1").setPort(9003));

        String cluster1 = "cluster1";
        ClusterMeta clusterMeta = new ClusterMeta(cluster1).setType("ONE_WAY");
        clusterMeta.setParent(jqDc);
        jqDc.addCluster(clusterMeta);
        clusterMeta.setActiveDc("jq");
        clusterMeta.setBackupDcs("oy");

        String cluster12 = "cluster1";
        ClusterMeta clusterMeta12 = new ClusterMeta(cluster12).setType("ONE_WAY");
        clusterMeta12.setParent(oyDc);
        oyDc.addCluster(clusterMeta12);
        clusterMeta12.setActiveDc("jq");
        clusterMeta12.setBackupDcs("oy");

        ShardMeta shardMeta1 = new ShardMeta("shard1");
        shardMeta1.setParent(clusterMeta);
        clusterMeta.addShard(shardMeta1);
        shardMeta1.addRedis(new RedisMeta().setIp("127.0.0.1").setPort(6379));
        shardMeta1.addRedis(new RedisMeta().setIp("127.0.0.2").setPort(6379));

        ShardMeta shardMeta12 = new ShardMeta("shard1");
        shardMeta12.setParent(clusterMeta12);
        clusterMeta12.addShard(shardMeta12);
        shardMeta12.addRedis(new RedisMeta().setIp("127.0.0.3").setPort(6379));
        shardMeta12.addRedis(new RedisMeta().setIp("127.0.0.4").setPort(6379));


        String cluster2 = "cluster2";
        ClusterMeta clusterMeta2 = new ClusterMeta(cluster2).setType("ONE_WAY");
        clusterMeta2.setParent(jqDc);
        jqDc.addCluster(clusterMeta2);
        clusterMeta2.setActiveDc("jq");
        clusterMeta2.setBackupDcs("oy");

        String cluster22 = "cluster2";
        ClusterMeta clusterMeta22 = new ClusterMeta(cluster22).setType("ONE_WAY");
        clusterMeta22.setParent(oyDc);
        oyDc.addCluster(clusterMeta22);
        clusterMeta22.setActiveDc("jq");
        clusterMeta22.setBackupDcs("oy");

        ShardMeta shardMeta2 = new ShardMeta("shard2");
        shardMeta2.setParent(clusterMeta2);
        clusterMeta2.addShard(shardMeta2);
        shardMeta2.addRedis(new RedisMeta().setIp("127.0.0.1").setPort(6380));
        shardMeta2.addRedis(new RedisMeta().setIp("127.0.0.2").setPort(6380));

        ShardMeta shardMeta22 = new ShardMeta("shard2");
        shardMeta22.setParent(clusterMeta22);
        clusterMeta22.addShard(shardMeta22);
        shardMeta22.addRedis(new RedisMeta().setIp("127.0.0.3").setPort(6380));
        shardMeta22.addRedis(new RedisMeta().setIp("127.0.0.4").setPort(6380));

        xpipeMeta.addDc(jqDc).addDc(oyDc);
        return xpipeMeta;
    }

    private List<DcTbl> toBuildTbl() {
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
    public void testFindAllDcsRichinfo() {
        when(dcMetaService.getDcMeta("jq")).thenReturn(xpipeMeta.findDc("jq"));
        when(dcMetaService.getDcMeta("oy")).thenReturn(xpipeMeta.findDc("oy"));
        dcService = spy(dcService);
        Mockito.doReturn(toBuildTbl()).when(dcService).findAllDcs();
        List<DcListDcModel> result = dcService.findAllDcsRichInfo();
        Assert.assertEquals(2, result.size());
        result.forEach(dcListDcModel -> {
            if (dcListDcModel.getDcName() == "jq") {
                Assert.assertEquals(5, dcListDcModel.getClusterTypes().size());
                dcListDcModel.getClusterTypes().forEach(model -> {
                    if (model.getClusterType().equals(ClusterType.ONE_WAY.name())) {
                        Assert.assertEquals(4, model.getRedisCount());
                        Assert.assertEquals(2, model.getClusterCount());
                        Assert.assertEquals(2, model.getClusterInActiveDcCount());
                    } else if (model.getClusterType().equals(ClusterType.SINGLE_DC.name())) {
                        Assert.assertEquals(2, model.getRedisCount());
                        Assert.assertEquals(1, model.getClusterCount());
                        Assert.assertEquals(1, model.getClusterInActiveDcCount());
                    } else if (model.getClusterType().equals(ClusterType.LOCAL_DC.name())) {
                        Assert.assertEquals(2, model.getRedisCount());
                        Assert.assertEquals(1, model.getClusterCount());
                        Assert.assertEquals(0, model.getClusterInActiveDcCount());
                    } else if (model.getClusterType().equals(ClusterType.CROSS_DC.name())) {
                        Assert.assertEquals(2, model.getRedisCount());
                        Assert.assertEquals(1, model.getClusterCount());
                        Assert.assertEquals(0, model.getClusterInActiveDcCount());
                    } else if (model.getClusterType().equals("")) {
                        Assert.assertEquals(10, model.getRedisCount());
                        Assert.assertEquals(5, model.getClusterCount());
                        Assert.assertEquals(3, model.getClusterInActiveDcCount());
                    } else {
                        Assert.fail("no cluster type matched");
                    }
                });

            } else if (dcListDcModel.getDcName() == "oy") {
                Assert.assertEquals(4, dcListDcModel.getClusterTypes().size());
                dcListDcModel.getClusterTypes().forEach(model -> {
                    if (model.getClusterType().equals(ClusterType.ONE_WAY.name())) {
                        Assert.assertEquals(4, model.getRedisCount());
                        Assert.assertEquals(2, model.getClusterCount());
                        Assert.assertEquals(0, model.getClusterInActiveDcCount());
                    } else if (model.getClusterType().equals(ClusterType.LOCAL_DC.name())) {
                        Assert.assertEquals(2, model.getRedisCount());
                        Assert.assertEquals(1, model.getClusterCount());
                        Assert.assertEquals(0, model.getClusterInActiveDcCount());
                    } else if (model.getClusterType().equals(ClusterType.CROSS_DC.name())) {
                        Assert.assertEquals(2, model.getRedisCount());
                        Assert.assertEquals(1, model.getClusterCount());
                        Assert.assertEquals(0, model.getClusterInActiveDcCount());
                    } else if (model.getClusterType().equals("")) {
                        Assert.assertEquals(8, model.getRedisCount());
                        Assert.assertEquals(4, model.getClusterCount());
                        Assert.assertEquals(0, model.getClusterInActiveDcCount());
                    } else {
                        Assert.fail("no cluster type matched");
                    }
                });
            }
        });
    }
}
