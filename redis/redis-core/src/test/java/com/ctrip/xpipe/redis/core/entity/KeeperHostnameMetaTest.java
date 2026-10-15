package com.ctrip.xpipe.redis.core.entity;

import com.ctrip.xpipe.redis.core.AbstractRedisTest;
import com.ctrip.xpipe.redis.core.meta.clone.MetaCloneFacade;
import com.ctrip.xpipe.redis.core.transform.DefaultSaxParser;
import org.junit.Assert;
import org.junit.Test;

/**
 * keepercontainer 容器化改造：KeeperMeta / KeeperContainerMeta 的 hostname 属性
 * 必须能穿过 meta 的实际传播通道（console 下发给 metaserver/checker 的 XML 序列化，
 * 以及 MetaCloneFacade 的深拷贝），否则容器 keeper 的身份会在链路中途静默丢失。
 */
public class KeeperHostnameMetaTest extends AbstractRedisTest {

    private static final String POD_HOSTNAME = "keepercontainer-pod-a-0";

    @Test
    public void testHostnameSurvivesXmlRoundTrip() throws Exception {
        String xml = "<xpipe>"
                + "  <dc id='jq'>"
                + "    <keeperContainers>"
                + "      <keeperContainer id='1' ip='0.0.0.0' port='8080' hostname='" + POD_HOSTNAME + "'/>"
                + "      <keeperContainer id='2' ip='1.1.1.1' port='8080'/>"
                + "    </keeperContainers>"
                + "    <cluster id='cluster1' active-dc='jq' type='one_way'>"
                + "      <shard id='shard1'>"
                + "        <keeper id='40a' ip='0.0.0.0' port='6380' active='true' keeperContainerId='1'"
                + "                hostname='" + POD_HOSTNAME + "'/>"
                + "        <keeper id='40b' ip='1.1.1.1' port='6380' active='false' keeperContainerId='2'/>"
                + "      </shard>"
                + "    </cluster>"
                + "  </dc>"
                + "</xpipe>";

        XpipeMeta parsed = DefaultSaxParser.parse(xml);
        DcMeta dcMeta = parsed.getDcs().get("jq");

        KeeperContainerMeta podContainer = findKeeperContainer(dcMeta, 1L);
        Assert.assertEquals(POD_HOSTNAME, podContainer.getHostname());

        // 物理机容器不带该属性，解析后应为 null，不能被误填
        KeeperContainerMeta physicalContainer = findKeeperContainer(dcMeta, 2L);
        Assert.assertNull(physicalContainer.getHostname());

        ShardMeta shardMeta = dcMeta.getClusters().get("cluster1").getShards().get("shard1");
        Assert.assertEquals(POD_HOSTNAME, shardMeta.getKeepers().get(0).getHostname());
        Assert.assertNull(shardMeta.getKeepers().get(1).getHostname());

        // 再序列化回 XML 并重新解析，确认属性能写出而非只能读入
        XpipeMeta reparsed = DefaultSaxParser.parse(parsed.toString());
        DcMeta reparsedDc = reparsed.getDcs().get("jq");
        Assert.assertEquals(POD_HOSTNAME, findKeeperContainer(reparsedDc, 1L).getHostname());
        Assert.assertEquals(POD_HOSTNAME,
                reparsedDc.getClusters().get("cluster1").getShards().get("shard1").getKeepers().get(0).getHostname());
    }

    @Test
    public void testHostnameSurvivesMetaClone() {
        KeeperMeta keeperMeta = new KeeperMeta().setIp("0.0.0.0").setPort(6380).setHostname(POD_HOSTNAME);
        keeperMeta.setId("40a");
        Assert.assertEquals(POD_HOSTNAME, MetaCloneFacade.INSTANCE.clone(keeperMeta).getHostname());

        KeeperContainerMeta containerMeta =
                new KeeperContainerMeta().setIp("0.0.0.0").setPort(8080).setHostname(POD_HOSTNAME);
        containerMeta.setId(1L);
        Assert.assertEquals(POD_HOSTNAME, MetaCloneFacade.INSTANCE.clone(containerMeta).getHostname());
    }

    private KeeperContainerMeta findKeeperContainer(DcMeta dcMeta, long id) {
        for (KeeperContainerMeta meta : dcMeta.getKeeperContainers()) {
            if (id == meta.getId()) return meta;
        }
        throw new AssertionError("keeperContainer not found: " + id);
    }
}
