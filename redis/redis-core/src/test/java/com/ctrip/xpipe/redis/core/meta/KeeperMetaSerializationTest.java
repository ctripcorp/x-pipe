package com.ctrip.xpipe.redis.core.meta;

import com.ctrip.xpipe.AbstractTest;
import com.ctrip.xpipe.api.codec.Codec;
import com.ctrip.xpipe.redis.core.entity.KeeperMeta;
import org.junit.Assert;
import org.junit.Test;

/**
 * 验证KeeperMeta序列化包含hostname字段
 */
public class KeeperMetaSerializationTest extends AbstractTest {

    @Test
    public void testKeeperMetaSerializationWithHostname() {
        KeeperMeta keeper = new KeeperMeta();
        keeper.setId("runid123");
        keeper.setIp("0.0.0.0");
        keeper.setPort(6380);
        keeper.setHostname("pod-a-0");
        keeper.setKeeperContainerId(1L);
        keeper.setPriority(100);
        
        // 序列化
        String json = Codec.DEFAULT.encode(keeper);
        logger.info("[testKeeperMetaSerializationWithHostname] json: {}", json);
        
        // 验证JSON包含hostname字段
        Assert.assertTrue(json.contains("\"hostname\":\"pod-a-0\""));
        Assert.assertTrue(json.contains("\"ip\":\"0.0.0.0\""));
        Assert.assertTrue(json.contains("\"port\":6380"));
        
        // 反序列化
        KeeperMeta decoded = Codec.DEFAULT.decode(json, KeeperMeta.class);
        Assert.assertEquals("pod-a-0", decoded.getHostname());
        Assert.assertEquals("0.0.0.0", decoded.getIp());
        Assert.assertEquals(Integer.valueOf(6380), decoded.getPort());
        Assert.assertEquals("runid123", decoded.getId());
        Assert.assertEquals(Long.valueOf(1L), decoded.getKeeperContainerId());
        Assert.assertEquals(Integer.valueOf(100), decoded.getPriority());
    }

    @Test
    public void testKeeperMetaSerializationWithRealIp() {
        // 物理机keeper: 有真实IP, hostname为空
        KeeperMeta keeper = new KeeperMeta();
        keeper.setId("runid456");
        keeper.setIp("192.168.1.1");
        keeper.setPort(6380);
        keeper.setKeeperContainerId(2L);
        
        String json = Codec.DEFAULT.encode(keeper);
        logger.info("[testKeeperMetaSerializationWithRealIp] json: {}", json);
        
        KeeperMeta decoded = Codec.DEFAULT.decode(json, KeeperMeta.class);
        Assert.assertNull(decoded.getHostname());
        Assert.assertEquals("192.168.1.1", decoded.getIp());
        Assert.assertEquals(Integer.valueOf(6380), decoded.getPort());
    }

    @Test
    public void testKeeperMetaEqualityAfterSerialization() {
        KeeperMeta original = new KeeperMeta()
                .setId("runid789")
                .setIp("0.0.0.0")
                .setPort(6380)
                .setHostname("pod-b-0")
                .setKeeperContainerId(3L)
                .setPriority(50);
        
        String json = Codec.DEFAULT.encode(original);
        KeeperMeta decoded = Codec.DEFAULT.decode(json, KeeperMeta.class);
        
        // 验证equals方法
        Assert.assertEquals(original, decoded);
    }
}
