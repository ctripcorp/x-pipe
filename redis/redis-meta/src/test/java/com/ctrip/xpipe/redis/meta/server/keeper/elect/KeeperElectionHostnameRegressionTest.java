package com.ctrip.xpipe.redis.meta.server.keeper.elect;

import com.ctrip.xpipe.codec.JsonCodec;
import com.ctrip.xpipe.redis.core.entity.ClusterMeta;
import com.ctrip.xpipe.redis.core.entity.KeeperMeta;
import com.ctrip.xpipe.redis.core.entity.ShardMeta;
import com.ctrip.xpipe.redis.meta.server.keeper.KeeperActiveElectAlgorithm;
import com.ctrip.xpipe.redis.meta.server.keeper.KeeperActiveElectAlgorithmManager;
import com.ctrip.xpipe.redis.meta.server.meta.CurrentMetaManager;
import com.ctrip.xpipe.redis.meta.server.meta.DcMetaCache;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

import static org.mockito.Mockito.*;

/**
 * Task 12 回归测试: 验证hostname不影响选举结果
 */
@RunWith(MockitoJUnitRunner.class)
public class KeeperElectionHostnameRegressionTest extends AbstractKeeperElectorManagerTest {

    @Mock
    private CurrentMetaManager currentMetaManager;

    @Mock
    private KeeperActiveElectAlgorithmManager keeperActiveElectAlgorithmManager;

    @Mock
    private DcMetaCache dcMetaCache;

    @Mock
    private KeeperActiveElectAlgorithm keeperActiveElectAlgorithm;

    private DefaultKeeperElectorManager keeperElectorManager;
    private ClusterMeta clusterMeta;
    private ShardMeta shardMeta;

    @Before
    public void beforeKeeperElectionHostnameRegressionTest() throws Exception {
        when(keeperActiveElectAlgorithmManager.get(anyLong(), anyLong())).thenReturn(keeperActiveElectAlgorithm);
        when(keeperActiveElectAlgorithm.select(anyLong(), anyLong(), anyList())).thenReturn(new KeeperMeta());

        keeperElectorManager = getBean(DefaultKeeperElectorManager.class);
        keeperElectorManager.initialize();

        keeperElectorManager.setCurrentMetaManager(currentMetaManager);
        keeperElectorManager.setKeeperActiveElectAlgorithmManager(keeperActiveElectAlgorithmManager);
        keeperElectorManager.setDcMetaCache(dcMetaCache);
        when(dcMetaCache.getShardKeepers(anyLong(), anyLong())).thenReturn(Collections.emptyList());

        clusterMeta = differentCluster(getDc());
        shardMeta = (ShardMeta) clusterMeta.getShards().values().toArray()[0];
    }

    /**
     * 测试: KeeperMeta带hostname不影响选举结果
     * ZK节点中的KeeperMeta包含hostname字段时,选举流程应该正常工作
     */
    @Test
    public void testHostnameDoesNotAffectElection() {
        String prefix = "/path";
        List<ChildData> dataList = new LinkedList<>();

        // 创建带hostname的容器keeper
        KeeperMeta keeper1 = new KeeperMeta()
                .setId("runid1")
                .setIp("0.0.0.0")
                .setPort(6380)
                .setHostname("pod-a-0")
                .setKeeperContainerId(1L)
                .setPriority(100);

        KeeperMeta keeper2 = new KeeperMeta()
                .setId("runid2")
                .setIp("0.0.0.0")
                .setPort(6380)
                .setHostname("pod-b-0")
                .setKeeperContainerId(2L)
                .setPriority(90);

        KeeperMeta keeper3 = new KeeperMeta()
                .setId("runid3")
                .setIp("0.0.0.0")
                .setPort(6380)
                .setHostname("pod-c-0")
                .setKeeperContainerId(3L)
                .setPriority(80);

        dataList.add(new ChildData(prefix + "/" + randomString(10) + "-latch-01", null,
                JsonCodec.INSTANCE.encodeAsBytes(keeper1)));
        dataList.add(new ChildData(prefix + "/" + randomString(10) + "-latch-02", null,
                JsonCodec.INSTANCE.encodeAsBytes(keeper2)));
        dataList.add(new ChildData(prefix + "/" + randomString(10) + "-latch-03", null,
                JsonCodec.INSTANCE.encodeAsBytes(keeper3)));

        // 执行选举
        keeperElectorManager.updateShardLeader(Collections.singletonList(dataList),
                clusterMeta.getDbId(), shardMeta.getDbId());

        // 验证选举算法被调用,且收到的keeper列表包含正确的hostname
        verify(keeperActiveElectAlgorithm).select(eq(clusterMeta.getDbId()), eq(shardMeta.getDbId()),
                argThat(keepers -> {
                    if (keepers == null || keepers.size() != 3) {
                        return false;
                    }
                    // 验证所有keeper都有hostname
                    for (KeeperMeta k : (List<KeeperMeta>) keepers) {
                        if (k.getHostname() == null || k.getHostname().isEmpty()) {
                            return false;
                        }
                        if (!k.getIp().equals("0.0.0.0")) {
                            return false;
                        }
                    }
                    return true;
                }));

        // 验证survive keepers被正确设置
        verify(currentMetaManager).setSurviveKeepers(eq(clusterMeta.getDbId()), eq(shardMeta.getDbId()),
                anyList(), any(KeeperMeta.class));
    }

    /**
     * 测试: 物理机keeper和容器keeper混合场景
     */
    @Test
    public void testMixedPhysicalAndContainerKeepers() {
        String prefix = "/path";
        List<ChildData> dataList = new LinkedList<>();

        // 物理机keeper (有真实IP)
        KeeperMeta physicalKeeper = new KeeperMeta()
                .setId("runid-physical")
                .setIp("192.168.1.1")
                .setPort(6380)
                .setKeeperContainerId(1L)
                .setPriority(100);

        // 容器keeper (IP为0.0.0.0, 有hostname)
        KeeperMeta containerKeeper = new KeeperMeta()
                .setId("runid-container")
                .setIp("0.0.0.0")
                .setPort(6380)
                .setHostname("pod-x-0")
                .setKeeperContainerId(2L)
                .setPriority(90);

        dataList.add(new ChildData(prefix + "/" + randomString(10) + "-latch-01", null,
                JsonCodec.INSTANCE.encodeAsBytes(physicalKeeper)));
        dataList.add(new ChildData(prefix + "/" + randomString(10) + "-latch-02", null,
                JsonCodec.INSTANCE.encodeAsBytes(containerKeeper)));

        // 执行选举
        keeperElectorManager.updateShardLeader(Collections.singletonList(dataList),
                clusterMeta.getDbId(), shardMeta.getDbId());

        // 验证选举正常执行
        verify(keeperActiveElectAlgorithm).select(eq(clusterMeta.getDbId()), eq(shardMeta.getDbId()),
                argThat(keepers -> {
                    if (keepers == null || keepers.size() != 2) {
                        return false;
                    }
                    List<KeeperMeta> keeperList = (List<KeeperMeta>) keepers;
                    // 第一个应该是物理机keeper
                    boolean hasPhysical = keeperList.stream().anyMatch(k -> "192.168.1.1".equals(k.getIp()));
                    // 第二个应该是容器keeper
                    boolean hasContainer = keeperList.stream().anyMatch(k -> "pod-x-0".equals(k.getHostname()));
                    return hasPhysical && hasContainer;
                }));
    }

    /**
     * 测试: KeeperMeta序列化/反序列化后hostname保持正确
     */
    @Test
    public void testKeeperMetaSerializationPreservesHostname() {
        KeeperMeta original = new KeeperMeta()
                .setId("runid-test")
                .setIp("0.0.0.0")
                .setPort(6380)
                .setHostname("pod-test-0")
                .setKeeperContainerId(1L)
                .setPriority(100);

        // 序列化
        byte[] serialized = JsonCodec.INSTANCE.encodeAsBytes(original);

        // 反序列化
        KeeperMeta deserialized = JsonCodec.INSTANCE.decode(new String(serialized), KeeperMeta.class);

        // 验证关键字段
        assert deserialized.getHostname().equals("pod-test-0") : "hostname should be preserved";
        assert deserialized.getIp().equals("0.0.0.0") : "ip should be preserved";
        assert deserialized.getPort().equals(6380) : "port should be preserved";
        assert deserialized.getId().equals("runid-test") : "id should be preserved";
        assert deserialized.getKeeperContainerId().equals(1L) : "keeperContainerId should be preserved";
    }
}
