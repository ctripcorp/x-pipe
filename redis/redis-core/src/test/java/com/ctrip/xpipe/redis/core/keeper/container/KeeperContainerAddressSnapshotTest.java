package com.ctrip.xpipe.redis.core.keeper.container;

import com.ctrip.xpipe.api.codec.Codec;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

public class KeeperContainerAddressSnapshotTest {

    private static final String POD_A = "keepercontainer-pod-a-0";
    private static final String POD_B = "keepercontainer-pod-b-0";

    /**
     * 服务端由 Spring MVC 的 Jackson 序列化，客户端由 RestTemplateFactory 的 ObjectMapper 反序列化，两者都是标准 bean 规则。
     * 用 Codec.DEFAULT 与一个只关掉未知字段校验的普通 ObjectMapper 各往返一次，固定字段名契约。
     */
    @Test
    public void testSerializationRoundTrip() throws Exception {
        KeeperContainerAddressSnapshot snapshot = sample();

        String encoded = Codec.DEFAULT.encode(snapshot);
        for (String field : new String[]{"liveHostnameToIp", "tombstones", "liveIpToHostname", "ip", "remainingMillis"}) {
            Assert.assertTrue("字段名契约: " + field + " in " + encoded, encoded.contains("\"" + field + "\""));
        }
        Assert.assertEquals(snapshot, Codec.DEFAULT.decode(encoded, KeeperContainerAddressSnapshot.class));

        ObjectMapper plain = new ObjectMapper().configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        Assert.assertEquals(snapshot, plain.readValue(plain.writeValueAsString(snapshot), KeeperContainerAddressSnapshot.class));
        Assert.assertEquals(snapshot, plain.readValue(encoded, KeeperContainerAddressSnapshot.class));
    }

    /** 缺字段或显式 null 都落成空 map，接收方无需判空。 */
    @Test
    public void testMissingOrNullFieldsBecomeEmpty() {
        KeeperContainerAddressSnapshot empty = Codec.DEFAULT.decode("{}", KeeperContainerAddressSnapshot.class);
        Assert.assertTrue(empty.getLiveHostnameToIp().isEmpty());
        Assert.assertTrue(empty.getTombstones().isEmpty());
        Assert.assertTrue(empty.getLiveIpToHostname().isEmpty());

        KeeperContainerAddressSnapshot nulls = Codec.DEFAULT.decode(
                "{\"liveHostnameToIp\":null,\"tombstones\":null,\"liveIpToHostname\":null}",
                KeeperContainerAddressSnapshot.class);
        Assert.assertEquals(new KeeperContainerAddressSnapshot(), nulls);
    }

    private KeeperContainerAddressSnapshot sample() {
        Map<String, String> live = new HashMap<>();
        live.put(POD_A, "10.0.0.5");
        Map<String, KeeperContainerAddressSnapshot.TombstoneEntry> tombstones = new HashMap<>();
        tombstones.put(POD_B, new KeeperContainerAddressSnapshot.TombstoneEntry("10.0.0.6", 123456L));
        Map<String, String> reverse = new HashMap<>();
        reverse.put("10.0.0.5", POD_A);
        reverse.put("10.0.0.9", POD_A);
        return new KeeperContainerAddressSnapshot(live, tombstones, reverse);
    }
}
