package com.ctrip.xpipe.redis.comparator.meta;

import com.ctrip.xpipe.AbstractTest;
import com.ctrip.xpipe.api.foundation.FoundationService;
import com.ctrip.xpipe.redis.comparator.config.ComparatorConfig;
import com.ctrip.xpipe.redis.core.entity.ClusterMeta;
import com.ctrip.xpipe.redis.core.entity.DcMeta;
import com.ctrip.xpipe.redis.core.entity.KeeperContainerMeta;
import com.ctrip.xpipe.redis.core.entity.KeeperMeta;
import com.ctrip.xpipe.redis.core.entity.ShardMeta;
import com.ctrip.xpipe.redis.core.entity.XpipeMeta;
import com.ctrip.xpipe.redis.core.exception.RedisRuntimeException;
import com.ctrip.xpipe.redis.core.service.AbstractService;
import com.ctrip.xpipe.redis.core.transform.DefaultSaxParser;
import net.jpountz.lz4.LZ4Factory;
import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import okhttp3.mockwebserver.RecordedRequest;
import okio.Buffer;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.springframework.http.HttpHeaders;
import org.springframework.web.client.HttpStatusCodeException;
import org.springframework.web.client.ResourceAccessException;
import org.springframework.web.client.RestTemplate;

import java.net.InetAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * T-MC.3 / AC-13 客户端：MockWebServer + LZ4；超时 / 非 200 / 空体不抛裸 NPE。
 */
public class ComparatorMetaServiceTest extends AbstractTest {

    private static final String DC = "jq";

    private static final String SOURCE =
            "src/main/java/com/ctrip/xpipe/redis/comparator/meta/ComparatorMetaService.java";

    private MockWebServer webServer;

    private String console;

    @Before
    public void setupComparatorMetaServiceTest() throws Exception {
        webServer = new MockWebServer();
        webServer.start(InetAddress.getByName("127.0.0.1"), randomPort());
        console = "http://127.0.0.1:" + webServer.getPort();
    }

    @After
    public void tearDownComparatorMetaServiceTest() throws Exception {
        if (webServer != null) {
            webServer.shutdown();
        }
    }

    @Test
    public void testLz4BodyParsesSameAsUncompressed() throws Exception {
        XpipeMeta expected = sampleXpipeMeta();
        String xml = expected.toString();
        byte[] raw = xml.getBytes(StandardCharsets.UTF_8);

        webServer.enqueue(new MockResponse()
                .setHeader(HttpHeaders.CONTENT_ENCODING, ComparatorMetaService.ACCEPT_ENCODING_LZ4)
                .setHeader(HttpHeaders.CONTENT_TYPE, "application/octet-stream")
                .setBody(new Buffer().write(lz4Compress(raw))));
        webServer.enqueue(new MockResponse()
                .setHeader(HttpHeaders.CONTENT_TYPE, "text/plain")
                .setBody(xml));

        ComparatorMetaService service = service();
        DcMeta fromLz4 = service.getCurrentDcMeta();
        DcMeta fromPlain = service.getCurrentDcMeta();
        DcMeta local = DefaultSaxParser.parse(xml).getDcs().get(DC);

        assertSameDc(local, fromLz4);
        assertSameDc(local, fromPlain);

        RecordedRequest lz4Req = webServer.takeRequest();
        Assert.assertEquals("GET", lz4Req.getMethod());
        Assert.assertEquals("/api/meta/" + DC + "/all?format=xml", lz4Req.getPath());
        Assert.assertEquals(ComparatorMetaService.ACCEPT_ENCODING_LZ4,
                lz4Req.getHeader(HttpHeaders.ACCEPT_ENCODING));
    }

    @Test
    public void testEmptyBodyThrowsNotNpe() throws Exception {
        webServer.enqueue(new MockResponse().setResponseCode(200).setBody(""));
        try {
            service().getCurrentDcMeta();
            Assert.fail();
        } catch (RedisRuntimeException e) {
            Assert.assertTrue(e.getMessage().contains("empty"));
            Assert.assertTrue(e.getMessage().contains(DC));
        } catch (NullPointerException e) {
            Assert.fail("empty body must not throw NPE");
        }
    }

    @Test
    public void testNon200ThrowsNotNpe() throws Exception {
        webServer.enqueue(new MockResponse().setResponseCode(500).setBody("boom"));
        try {
            service().getCurrentDcMeta();
            Assert.fail();
        } catch (HttpStatusCodeException e) {
            Assert.assertEquals(500, e.getRawStatusCode());
        } catch (NullPointerException e) {
            Assert.fail("non-200 must not throw NPE");
        }
    }

    @Test
    public void testReadTimeoutThrowsNotNpe() throws Exception {
        webServer.enqueue(new MockResponse()
                .setBodyDelay(1, TimeUnit.SECONDS)
                .setBody(sampleXpipeMeta().toString()));
        ComparatorMetaService fast = new ComparatorMetaService(
                new ConsoleAddressConfig(console), new StubFoundation(),
                0, 10, 200, 200);
        try {
            fast.getCurrentDcMeta();
            Assert.fail();
        } catch (ResourceAccessException e) {
            Assert.assertNotNull(e.getMessage());
        } catch (NullPointerException e) {
            Assert.fail("timeout must not throw NPE");
        }
    }

    @Test
    public void testClientHasConnectAndReadTimeout() {
        ComparatorMetaService service = new ComparatorMetaService(new ComparatorConfig());
        Assert.assertTrue(service instanceof AbstractService);
        Assert.assertNotNull(service.restOperations());
        Assert.assertFalse(service.restOperations() instanceof RestTemplate);
        Assert.assertTrue(AbstractService.DEFAULT_SO_TIMEOUT > 0);
        Assert.assertTrue(AbstractService.FAST_CONNECT_TIMEOUT > 0);
    }

    @Test
    public void testEmptyConsoleAddressThrows() throws Exception {
        try {
            new ComparatorMetaService(new ConsoleAddressConfig(""), new StubFoundation()).getCurrentDcMeta();
            Assert.fail();
        } catch (IllegalStateException e) {
            Assert.assertTrue(e.getMessage().contains("console.address"));
        }
    }

    @Test
    public void testTfsJudgementThreeInputs() {
        ComparatorMetaService service = new ComparatorMetaService(new ComparatorConfig());
        DcMeta dcMeta = new DcMeta(DC);
        dcMeta.addKeeperContainer(container(1L, "tfs-1"));
        dcMeta.addKeeperContainer(container(2L, "DEFAULT"));
        Map<Long, KeeperContainerMeta> index = service.indexKeeperContainers(dcMeta);

        Assert.assertTrue(service.isTfsKeeper(keeper(1L), index));
        Assert.assertFalse(service.isTfsKeeper(keeper(2L), index));
        Assert.assertFalse(service.isTfsKeeper(keeper(99L), index));

        ShardMeta shard = new ShardMeta("shard1");
        shard.addKeeper(keeper(1L));
        shard.addKeeper(keeper(2L));
        shard.addKeeper(keeper(99L));
        List<KeeperMeta> tfs = service.listTfsKeepers(shard, index);
        Assert.assertEquals(1, tfs.size());
        Assert.assertEquals(Long.valueOf(1L), tfs.get(0).getKeeperContainerId());
    }

    @Test
    public void testIndexSkipsNullIdAndNullDcThrows() {
        ComparatorMetaService service = new ComparatorMetaService(new ComparatorConfig());
        try {
            service.indexKeeperContainers(null);
            Assert.fail();
        } catch (IllegalArgumentException e) {
            Assert.assertTrue(e.getMessage().contains("dcMeta"));
        }
        DcMeta dcMeta = new DcMeta(DC);
        dcMeta.addKeeperContainer(container(3L, "tfs"));
        dcMeta.addKeeperContainer(new KeeperContainerMeta().setDiskType("tfs"));
        Map<Long, KeeperContainerMeta> index = service.indexKeeperContainers(dcMeta);
        Assert.assertEquals(1, index.size());
        Assert.assertTrue(index.containsKey(3L));
    }

    @Test
    public void testSourceUsesKeeperDiskTypeUtilsNotTfsKeeperUtils() throws Exception {
        String text = new String(Files.readAllBytes(Paths.get(SOURCE)), StandardCharsets.UTF_8)
                .replaceAll("(?s)/\\*.*?\\*/", " ");
        Assert.assertFalse(text.contains("import com.ctrip.xpipe.redis.meta"));
        Assert.assertTrue(text.contains("import com.ctrip.xpipe.redis.core.keeper.KeeperDiskTypeUtils"));
        Assert.assertFalse(text.contains("new RestTemplate("));
        Assert.assertTrue(text.contains("extends AbstractService"));
        Assert.assertTrue(text.contains("ACCEPT_ENCODING"));
        Assert.assertTrue(text.contains("PATH_GET_DC_ALL_META"));
    }

    private ComparatorMetaService service() {
        return new ComparatorMetaService(new ConsoleAddressConfig(console), new StubFoundation());
    }

    private static void assertSameDc(DcMeta expected, DcMeta actual) {
        Assert.assertEquals(expected.getId(), actual.getId());
        Assert.assertEquals(expected.getKeeperContainers().size(), actual.getKeeperContainers().size());
        for (int i = 0; i < expected.getKeeperContainers().size(); i++) {
            KeeperContainerMeta exp = expected.getKeeperContainers().get(i);
            KeeperContainerMeta act = actual.getKeeperContainers().get(i);
            Assert.assertEquals(exp.getId(), act.getId());
            Assert.assertEquals(exp.getDiskType(), act.getDiskType());
            Assert.assertEquals(exp.getIp(), act.getIp());
        }
    }

    private static XpipeMeta sampleXpipeMeta() {
        DcMeta dcMeta = new DcMeta(DC);
        dcMeta.addKeeperContainer(container(1L, "tfs-1").setIp("10.0.0.1").setPort(8080));
        dcMeta.addKeeperContainer(container(2L, "DEFAULT").setIp("10.0.0.2").setPort(8080));
        ShardMeta shard = new ShardMeta("shard1").setDbId(11L);
        shard.addKeeper(keeper(1L).setIp("10.0.0.1").setPort(6380));
        shard.addKeeper(keeper(2L).setIp("10.0.0.2").setPort(6380));
        dcMeta.addCluster(new ClusterMeta("cluster1").addShard(shard));
        return new XpipeMeta().addDc(dcMeta);
    }

    private static KeeperContainerMeta container(long id, String diskType) {
        return new KeeperContainerMeta().setId(id).setDiskType(diskType);
    }

    private static KeeperMeta keeper(long containerId) {
        return new KeeperMeta().setKeeperContainerId(containerId);
    }

    private static byte[] lz4Compress(byte[] raw) {
        net.jpountz.lz4.LZ4Compressor compressor = LZ4Factory.fastestInstance().fastCompressor();
        int max = compressor.maxCompressedLength(raw.length);
        byte[] compressed = new byte[max];
        int len = compressor.compress(raw, 0, raw.length, compressed, 0, max);
        return Arrays.copyOf(compressed, len);
    }

    static final class ConsoleAddressConfig extends ComparatorConfig {

        private final String address;

        ConsoleAddressConfig(String address) {
            this.address = address;
        }

        @Override
        public String getConsoleAddress() {
            return address;
        }
    }

    static final class StubFoundation implements FoundationService {

        @Override
        public String getDataCenter() {
            return DC;
        }

        @Override
        public String getAppId() {
            return "test";
        }

        @Override
        public String getLocalIp() {
            return "127.0.0.1";
        }

        @Override
        public String getHostName() {
            return "me";
        }

        @Override
        public String getGroupId() {
            return "group1";
        }

        @Override
        public String getRegion() {
            return "sha";
        }

        @Override
        public int getOrder() {
            return 0;
        }
    }
}
