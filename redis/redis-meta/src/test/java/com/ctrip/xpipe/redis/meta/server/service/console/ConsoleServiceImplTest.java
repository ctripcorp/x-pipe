package com.ctrip.xpipe.redis.meta.server.service.console;

import com.ctrip.xpipe.AbstractTest;
import com.ctrip.xpipe.redis.core.entity.*;
import com.ctrip.xpipe.retry.RetryPolicyFactories;
import com.ctrip.xpipe.spring.RestTemplateFactory;
import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import org.junit.Before;
import org.junit.Test;
import org.springframework.web.client.RestOperations;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.Assert.*;

/**
 * @author ayq
 * <p>
 * 2022/6/24 12:17
 */
public class ConsoleServiceImplTest extends AbstractTest {

    private MockWebServer webServer;

    private int port;

    private String dataFromConsole = "{\"clusters\":{\"hetero2-local-cluster\":{\"activeDc\":\"jq\",\"shards\":{\"hetero2-local-cluster_oy_1\":{\"redises\":[{\"port\":6381,\"ip\":\"127.0.0.1\",\"gid\":null,\"gtid\":null,\"master\":\"0.0.0.0:0\",\"sid\":null,\"survive\":null,\"id\":\"unknown\",\"offset\":null},{\"port\":6481,\"ip\":\"127.0.0.1\",\"gid\":null,\"gtid\":null,\"master\":\"0.0.0.0:0\",\"sid\":null,\"survive\":null,\"id\":\"unknown\",\"offset\":null}],\"keepers\":[],\"id\":\"hetero2-local-cluster_oy_1\",\"dbId\":32,\"phase\":null,\"appliers\":[],\"sentinelId\":3,\"sentinelMonitorName\":\"hetero2-local-cluster+hetero2-local-cluster_oy_1+oy\"}},\"dcs\":null,\"id\":\"hetero2-local-cluster\",\"dbId\":10,\"type\":\"hetero\",\"activeRedisCheckRules\":null,\"adminEmails\":\" \",\"backupDcs\":\"\",\"clusterDesignatedRouteIds\":\"\",\"downstreamDcs\":null,\"orgId\":1,\"phase\":null,\"sources\":[{\"srcDc\":\"jq\",\"upstreamDc\":\"jq\",\"region\":\"SHA\",\"shards\":{\"hetero2-local-cluster_1\":{\"redises\":[],\"keepers\":[{\"port\":9100,\"ip\":\"127.0.0.1\",\"master\":null,\"survive\":null,\"id\":\"ffffffffffffffffffffffffffffffffffffffff\",\"active\":false,\"keeperContainerId\":4,\"offset\":null},{\"port\":9101,\"ip\":\"127.0.0.1\",\"master\":null,\"survive\":null,\"id\":\"ffffffffffffffffffffffffffffffffffffffff\",\"active\":false,\"keeperContainerId\":5,\"offset\":null}],\"id\":\"hetero2-local-cluster_1\",\"dbId\":30,\"phase\":null,\"appliers\":[{\"applierContainerId\":5,\"port\":16200,\"ip\":\"127.0.0.1\",\"id\":null,\"active\":true},{\"applierContainerId\":6,\"port\":16201,\"ip\":\"127.0.0.1\",\"id\":null,\"active\":false}],\"sentinelId\":null,\"sentinelMonitorName\":null},\"hetero2-local-cluster_2\":{\"redises\":[],\"keepers\":[{\"port\":9200,\"ip\":\"127.0.0.1\",\"master\":null,\"survive\":null,\"id\":\"ffffffffffffffffffffffffffffffffffffffff\",\"active\":false,\"keeperContainerId\":4,\"offset\":null},{\"port\":9201,\"ip\":\"127.0.0.1\",\"master\":null,\"survive\":null,\"id\":\"ffffffffffffffffffffffffffffffffffffffff\",\"active\":false,\"keeperContainerId\":5,\"offset\":null}],\"id\":\"hetero2-local-cluster_2\",\"dbId\":31,\"phase\":null,\"appliers\":[{\"applierContainerId\":5,\"port\":16202,\"ip\":\"127.0.0.1\",\"id\":null,\"active\":true},{\"applierContainerId\":6,\"port\":16203,\"ip\":\"127.0.0.1\",\"id\":null,\"active\":false}],\"sentinelId\":null,\"sentinelMonitorName\":null}}}],\"allShards\":{\"hetero2-local-cluster_oy_1\":{\"redises\":[{\"port\":6381,\"ip\":\"127.0.0.1\",\"gid\":null,\"gtid\":null,\"master\":\"0.0.0.0:0\",\"sid\":null,\"survive\":null,\"id\":\"unknown\",\"offset\":null},{\"port\":6481,\"ip\":\"127.0.0.1\",\"gid\":null,\"gtid\":null,\"master\":\"0.0.0.0:0\",\"sid\":null,\"survive\":null,\"id\":\"unknown\",\"offset\":null}],\"keepers\":[],\"id\":\"hetero2-local-cluster_oy_1\",\"dbId\":32,\"phase\":null,\"appliers\":[],\"sentinelId\":3,\"sentinelMonitorName\":\"hetero2-local-cluster+hetero2-local-cluster_oy_1+oy\"},\"hetero2-local-cluster_1\":{\"redises\":[],\"keepers\":[{\"port\":9100,\"ip\":\"127.0.0.1\",\"master\":null,\"survive\":null,\"id\":\"ffffffffffffffffffffffffffffffffffffffff\",\"active\":false,\"keeperContainerId\":4,\"offset\":null},{\"port\":9101,\"ip\":\"127.0.0.1\",\"master\":null,\"survive\":null,\"id\":\"ffffffffffffffffffffffffffffffffffffffff\",\"active\":false,\"keeperContainerId\":5,\"offset\":null}],\"id\":\"hetero2-local-cluster_1\",\"dbId\":30,\"phase\":null,\"appliers\":[{\"applierContainerId\":5,\"port\":16200,\"ip\":\"127.0.0.1\",\"id\":null,\"active\":true},{\"applierContainerId\":6,\"port\":16201,\"ip\":\"127.0.0.1\",\"id\":null,\"active\":false}],\"sentinelId\":null,\"sentinelMonitorName\":null},\"hetero2-local-cluster_2\":{\"redises\":[],\"keepers\":[{\"port\":9200,\"ip\":\"127.0.0.1\",\"master\":null,\"survive\":null,\"id\":\"ffffffffffffffffffffffffffffffffffffffff\",\"active\":false,\"keeperContainerId\":4,\"offset\":null},{\"port\":9201,\"ip\":\"127.0.0.1\",\"master\":null,\"survive\":null,\"id\":\"ffffffffffffffffffffffffffffffffffffffff\",\"active\":false,\"keeperContainerId\":5,\"offset\":null}],\"id\":\"hetero2-local-cluster_2\",\"dbId\":31,\"phase\":null,\"appliers\":[{\"applierContainerId\":5,\"port\":16202,\"ip\":\"127.0.0.1\",\"id\":null,\"active\":true},{\"applierContainerId\":6,\"port\":16203,\"ip\":\"127.0.0.1\",\"id\":null,\"active\":false}],\"sentinelId\":null,\"sentinelMonitorName\":null}},\"lastModifiedTime\":null}}}";

    @Before
    public void setUp() throws Exception {
        webServer = new MockWebServer();
        port = randomPort();
        webServer.start(port);
    }

    @Test
    public void testDownstreamDcMetaXmlToObject() {
        webServer.enqueue(new MockResponse().setBody(dataFromConsole).setHeader("Content-Type", "application/json"));

        RestOperations restTemplate = RestTemplateFactory.createCommonsHttpRestTemplate(
                1000,
                10000,
                1200,
                6000,
                1,
                RetryPolicyFactories.newRestOperationsRetryPolicyFactory(5));

        DcMeta dcMeta = restTemplate.getForObject("http://127.0.0.1:" + port, DcMeta.class);
        for (ClusterMeta clusterMeta : dcMeta.getClusters().values()) {
            Map<String, ShardMeta> shardMetaMap = clusterMeta.getShards();
            assertEquals(1, shardMetaMap.size());
            for (ShardMeta shardMeta : shardMetaMap.values()) {
                assertEquals(2, shardMeta.getRedises().size());
                assertEquals(0, shardMeta.getKeepers().size());
                assertEquals(0, shardMeta.getAppliers().size());
            }

            List<SourceMeta> sourceMetaList = clusterMeta.getSources();
            assertEquals(1, sourceMetaList.size());
            Map<String, ShardMeta> sourceShardMetaMap = sourceMetaList.get(0).getShards();
            for (ShardMeta shardMeta : sourceShardMetaMap.values()) {
                assertEquals(0, shardMeta.getRedises().size());
                assertEquals(2, shardMeta.getKeepers().size());
                assertEquals(2, shardMeta.getAppliers().size());
            }
        }
    }
}
