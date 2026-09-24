package com.ctrip.xpipe.redis.comparator;

import com.ctrip.xpipe.AbstractTest;
import com.ctrip.xpipe.foundation.DefaultFoundationService;
import com.ctrip.xpipe.redis.comparator.compare.CompareLane;
import com.ctrip.xpipe.redis.comparator.compare.ShardComparator;
import com.ctrip.xpipe.redis.comparator.compare.ShardComparator.CompareOnceResult;
import com.ctrip.xpipe.redis.comparator.config.ComparatorConfig;
import com.ctrip.xpipe.redis.comparator.meta.ComparatorMetaService;
import com.ctrip.xpipe.redis.comparator.report.CompareMetricsCollector;
import com.ctrip.xpipe.redis.comparator.report.CompareReporter;
import com.ctrip.xpipe.redis.comparator.stream.StreamRingBuffer;
import com.ctrip.xpipe.redis.comparator.stream.StreamRingBuffer.PeekStatus;
import com.ctrip.xpipe.netty.commands.ByteBufReceiver;
import com.ctrip.xpipe.redis.core.exception.RdbRejectedException;
import com.ctrip.xpipe.redis.core.protocal.cmd.CmdTailGapAllowedSync;
import com.ctrip.xpipe.redis.core.service.AbstractService;
import com.ctrip.xpipe.utils.StringUtil;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

/**
 * Phase CA: stage-2 cross-cutting acceptance. Phase-local ACs stay in their Phase tests.
 * <p>
 * T-CA.7 手工清单（不进 CI）：真实 TFS 分片 2 路 keeper 上跑通
 * 「建流 → 比对 → 注入失配 → realign」；任意 comparator 异常不改 Keeper
 * {@code SETSTATE PREPARE} 回包、不触发生产断链。
 */
public class ComparatorStageTwoAcceptanceTest extends AbstractTest {

    @Test
    public void testAc13_consoleReturnsUtf8BytesFromMetaCache() throws Exception {
        String controller = read(
                "..", "redis-console", "src", "main", "java",
                "com", "ctrip", "xpipe", "redis", "console", "controller", "api", "checker",
                "ConsoleCheckerController.java");
        String method = methodBody(controller,
                "public byte[] getDcAllMeta(@PathVariable String dcName");
        Assert.assertTrue(method.contains("metaCache.getXpipeMeta()"));
        Assert.assertFalse(method.contains("dcMetaService"));
        int utf8 = count(method, "getBytes(StandardCharsets.UTF_8)");
        Assert.assertEquals("only the two return branches may encode", 2, utf8);

        Assert.assertTrue(AbstractService.class.isAssignableFrom(ComparatorMetaService.class));
        String client = readMain("meta", "ComparatorMetaService.java");
        Assert.assertTrue(client.contains("ACCEPT_ENCODING_LZ4"));
        Assert.assertTrue(client.contains("extends AbstractService"));
        Assert.assertFalse(client.contains("new RestTemplate("));
    }

    @Test
    public void testAc13b_getHostNameThreeImplementations() throws Exception {
        String hostName = new DefaultFoundationService().getHostName();
        Assert.assertFalse(StringUtil.isEmpty(hostName));

        String api = read("..", "..", "core", "src", "main", "java",
                "com", "ctrip", "xpipe", "api", "foundation", "FoundationService.java");
        Assert.assertTrue(api.contains("String getHostName();"));

        String def = read("..", "..", "core", "src", "main", "java",
                "com", "ctrip", "xpipe", "foundation", "DefaultFoundationService.java");
        Assert.assertTrue(methodBody(def, "public String getHostName()").contains("getLocalHost()"));

        String ctrip = read("..", "..", "services", "ctrip-service", "src", "main", "java",
                "com", "ctrip", "xpipe", "service", "foundation", "CtripFoundationService.java");
        Assert.assertTrue(methodBody(ctrip, "public String getHostName()")
                .contains("Foundation.net().getHostName()"));

        String test = read("..", "redis-integration-test", "src", "test", "java",
                "com", "ctrip", "xpipe", "redis", "integratedtest", "console", "config",
                "TestFoundationService.java");
        Assert.assertTrue(methodBody(test, "public String getHostName()").contains("HOST_NAME_KEY"));
    }

    @Test
    public void testAc16_cmdTailAlwaysQuestionMinusFourAndRejectsRdb() throws Exception {
        AtomicInteger commands = new AtomicInteger();
        CmdTailGapAllowedSync sync = new CmdTailGapAllowedSync(null, buf -> commands.incrementAndGet(), scheduled);
        assertCmdTailRequest(sync);
        sync.receive(null, Unpooled.wrappedBuffer(
                "+CONTINUE 0123456789012345678901234567890123456789 100\r\n".getBytes(StandardCharsets.US_ASCII)));
        assertCmdTailRequest(sync);
        Assert.assertEquals(0, commands.get());

        CmdTailGapAllowedSync full = new CmdTailGapAllowedSync(null, buf -> commands.incrementAndGet(), scheduled);
        full.getRequest().release();
        ByteBufReceiver.RECEIVER_RESULT result = full.receive(null, Unpooled.wrappedBuffer(
                "+FULLRESYNC 0123456789012345678901234567890123456789 1\r\n"
                        .getBytes(StandardCharsets.US_ASCII)));
        Assert.assertEquals(ByteBufReceiver.RECEIVER_RESULT.FAIL, result);
        Assert.assertTrue(full.future().isDone());
        Assert.assertFalse(full.future().isSuccess());
        Assert.assertTrue(full.future().cause() instanceof RdbRejectedException);
        Assert.assertEquals(0, commands.get());

        String source = read("..", "redis-core", "src", "main", "java",
                "com", "ctrip", "xpipe", "redis", "core", "protocal", "cmd",
                "CmdTailGapAllowedSync.java");
        Assert.assertTrue(methodBody(source, "protected RdbBulkStringParser createRdbReader()")
                .contains("rejectRdb"));
        Assert.assertTrue(methodBody(source, "protected void failReadRdb(Throwable throwable)")
                .contains("rejectRdb"));
        Assert.assertTrue(methodBody(source, "protected void doOnFullSync()").contains("rejectRdb"));
        Assert.assertTrue(methodBody(source, "protected void doOnXFullSync()").contains("rejectRdb"));
    }

    @Test
    public void testAc16b_parentSyncClassesUnchanged() throws Exception {
        String cmdTail = read("..", "redis-core", "src", "main", "java",
                "com", "ctrip", "xpipe", "redis", "core", "protocal", "cmd",
                "CmdTailGapAllowedSync.java");
        Assert.assertFalse(cmdTail.contains("ReplicationStore"));
        Assert.assertFalse(cmdTail.contains("RdbStore"));

        for (String name : Arrays.asList(
                "AbstractGapAllowedSync.java",
                "InMemoryGapAllowedSync.java",
                "PartialOnlyGapAllowedSync.java",
                "Replconf.java")) {
            String text = read("..", "redis-core", "src", "main", "java",
                    "com", "ctrip", "xpipe", "redis", "core", "protocal", "cmd", name);
            Assert.assertFalse(name + " must stay free of cmd-tail client",
                    text.contains("CmdTailGapAllowedSync")
                            || text.contains("CmdTailStreamListener")
                            || text.contains("redis.comparator")
                            || text.contains("KEEPER_CMD_TAIL"));
        }
    }

    @Test
    public void testAc17_overwriteConsumesInboundAndNeverPinsKeeperGc() throws Exception {
        StreamRingBuffer buffer = new StreamRingBuffer(8, 0L);
        ByteBuf first = Unpooled.wrappedBuffer("abcdefgh".getBytes(StandardCharsets.US_ASCII));
        ByteBuf extra = Unpooled.wrappedBuffer("ijkl".getBytes(StandardCharsets.US_ASCII));
        try {
            buffer.write(first);
            Assert.assertEquals(0, first.readableBytes());
            buffer.write(extra);
            Assert.assertEquals(0, extra.readableBytes());
        } finally {
            first.release();
            extra.release();
        }
        Assert.assertEquals(4L, buffer.getBufferStart());
        Assert.assertEquals(PeekStatus.OVERWRITTEN, buffer.peek(0L, 4, new byte[4]));

        walkMain(text -> {
            Assert.assertFalse(text.contains("setAutoRead"));
            Assert.assertFalse(text.contains("AUTO_READ"));
            Assert.assertFalse(text.contains("lowestReadingOffset"));
            Assert.assertFalse(text.contains("addCommandsListener"));
        });
    }

    @Test
    public void testAc18_compareAdvancesOnSlowestLane() {
        FakeLane fast = new FakeLane("10.0.0.1:6380", 100L, 32);
        FakeLane slow = new FakeLane("10.0.0.2:6380", 100L, 32);
        fast.write(seq(16));
        slow.write(seq(4));
        ShardComparator cmp = new ShardComparator("c1", "s1", Arrays.asList(fast, slow),
                chunk(8), CompareReporter.NOOP);
        Assert.assertEquals(CompareOnceResult.ADVANCED, cmp.compareOnce());
        Assert.assertEquals(4L, cmp.getComparedBytes());
        Assert.assertEquals(0, cmp.getMismatchCount());
        Assert.assertEquals(CompareOnceResult.NO_DATA, cmp.compareOnce());
        Assert.assertEquals(4L, cmp.getComparedBytes());
    }

    @Test
    public void testAc14b_ioPathAndStopDoNotBlock() throws Exception {
        String stream = readMain("stream", "KeeperReplStream.java");
        String onCommands = methodBody(stream, "public void onCommands(ByteBuf buf)");
        Assert.assertTrue(onCommands.contains("target.write(buf)"));
        Assert.assertTrue(onCommands.contains("wakeCompareThread()"));
        Assert.assertFalse(onCommands.contains("compareOnce"));
        Assert.assertFalse(onCommands.contains("MetricProxy"));
        Assert.assertFalse(onCommands.contains("Thread.sleep"));
        Assert.assertFalse(onCommands.contains(".await("));

        String comparator = readMain("compare", "ShardComparator.java");
        String stop = methodBody(comparator, "public void stop()");
        Assert.assertFalse(stop.contains("notify"));
        Assert.assertFalse(stop.contains(".join("));
        Assert.assertFalse(stop.contains("signal"));
        Assert.assertTrue(comparator.contains("COMPARE_WAIT_MILLI"));
        Assert.assertTrue(comparator.contains("XpipeThreadFactory"));
    }

    @Test
    public void testAc14And15_taskAndAssignerContracts() throws Exception {
        String manager = readMain("meta", "ShardCompareTaskManager.java");
        Assert.assertTrue(manager.contains("EVENT_INSUFFICIENT_LANES"));
        Assert.assertTrue(manager.contains("isMine("));
        Assert.assertTrue(manager.contains("listTfsKeepers"));
        Assert.assertFalse(manager.contains("TfsKeeperUtils"));
        Assert.assertTrue(manager.contains("catch (Throwable"));
        String meta = readMain("meta", "ComparatorMetaService.java");
        Assert.assertTrue(meta.contains("KeeperDiskTypeUtils"));
        Assert.assertFalse(meta.contains("import com.ctrip.xpipe.redis.meta"));

        String assigner = readMain("balance", "CompareTaskAssigner.java");
        Assert.assertTrue(assigner.contains("ServerGroupProvider"));
        Assert.assertTrue(assigner.contains("applyUnconfiguredFallback"));
        Assert.assertTrue(assigner.contains("EVENT_CMS_UNREACHABLE"));
        Assert.assertTrue(assigner.contains("EVENT_CMS_FIRST_FAIL"));
        Assert.assertTrue(assigner.contains("EVENT_HOST_NOT_IN_GROUP"));
        Assert.assertTrue(assigner.contains("firstLabel"));
        Assert.assertTrue(assigner.contains("catch (Throwable"));
        Assert.assertFalse(assigner.contains("new RestTemplate("));
        Assert.assertFalse(assigner.contains("CmsServerGroupProvider"));
    }

    @Test
    public void testAc22_hickwallOnlyComparedBytesOutsideCompareLoop() throws Exception {
        Assert.assertEquals("comparedBytes", CompareMetricsCollector.METRIC_COMPARED_BYTES);
        String collector = readMain("report", "CompareMetricsCollector.java");
        Assert.assertTrue(collector.contains("writeBinMultiDataPoint"));
        Assert.assertFalse(collector.contains("logger.error"));
        Assert.assertEquals(1, count(collector, "new MetricData("));
        Assert.assertEquals(1, count(collector, "new MetricData(METRIC_COMPARED_BYTES"));
        Assert.assertFalse(collector.contains("new MetricData(\""));
        String writeCompared = methodBody(collector, "private void writeComparedBytes");
        Assert.assertTrue(writeCompared.contains("new MetricData(METRIC_COMPARED_BYTES"));
        walkMain(text -> {
            if (text.contains("class CompareMetricsCollector")) {
                return;
            }
            Assert.assertFalse(text.contains("import com.ctrip.xpipe.metric"));
            Assert.assertFalse(text.contains("writeBinMultiDataPoint"));
        });
    }

    @Test
    public void testAc21_comparatorNeverTouchesKeeperSetState() throws Exception {
        walkMain(text -> {
            String stripped = stripComments(text);
            Assert.assertFalse(stripped.contains("SETSTATE"));
            Assert.assertFalse(stripped.contains("setstate"));
            Assert.assertFalse(stripped.contains("SET_STATE"));
            Assert.assertFalse(stripped.contains("KeeperSetStateCommand"));
            Assert.assertFalse(stripped.contains("ApplierSetStateCommand"));
            Assert.assertFalse(stripped.contains("AbstractKeeperCommand"));
            Assert.assertFalse(stripped.contains("AbstractApplierCommand"));
            Assert.assertFalse(stripped.contains("KeeperCommandHandler"));
            Assert.assertFalse(stripped.contains("RedisKeeperServerState"));
        });
    }

    private static void assertCmdTailRequest(CmdTailGapAllowedSync sync) {
        ByteBuf formatted = sync.getRequest();
        try {
            Assert.assertEquals("PSYNC ? -4\r\n", formatted.toString(StandardCharsets.US_ASCII));
        } finally {
            formatted.release();
        }
    }

    private static ComparatorConfig chunk(int bytes) {
        return new ComparatorConfig() {
            @Override
            public int getCompareChunkBytes() {
                return bytes;
            }
        };
    }

    private static byte[] seq(int len) {
        byte[] out = new byte[len];
        for (int i = 0; i < len; i++) {
            out[i] = (byte) i;
        }
        return out;
    }

    private static void walkMain(SourceAssertion assertion) throws Exception {
        Path root = Paths.get("src", "main", "java");
        Assert.assertTrue(root.toString(), Files.isDirectory(root));
        try (Stream<Path> stream = Files.walk(root)) {
            stream.filter(path -> path.toString().endsWith(".java")).forEach(path -> {
                try {
                    assertion.check(new String(Files.readAllBytes(path), StandardCharsets.UTF_8));
                } catch (IOException e) {
                    throw new IllegalStateException(path.toString(), e);
                }
            });
        }
    }

    private static String readMain(String... parts) throws IOException {
        Path path = Paths.get("src", "main", "java", "com", "ctrip", "xpipe", "redis", "comparator");
        for (String part : parts) {
            path = path.resolve(part);
        }
        Assert.assertTrue(path.toString(), Files.isRegularFile(path));
        return new String(Files.readAllBytes(path), StandardCharsets.UTF_8);
    }

    private static String read(String... parts) throws IOException {
        Path path = Paths.get(parts[0], Arrays.copyOfRange(parts, 1, parts.length));
        Assert.assertTrue(path.toString(), Files.isRegularFile(path));
        return new String(Files.readAllBytes(path), StandardCharsets.UTF_8);
    }

    private static String methodBody(String text, String signature) {
        int start = text.indexOf(signature);
        Assert.assertTrue(signature, start >= 0);
        int brace = text.indexOf('{', start);
        Assert.assertTrue(signature, brace > start);
        int depth = 0;
        for (int i = brace; i < text.length(); i++) {
            char c = text.charAt(i);
            if (c == '{') {
                depth++;
            } else if (c == '}') {
                depth--;
                if (depth == 0) {
                    return text.substring(brace, i + 1);
                }
            }
        }
        throw new AssertionError("unclosed " + signature);
    }

    private static int count(String text, String token) {
        int n = 0;
        for (int from = 0; (from = text.indexOf(token, from)) >= 0; from += token.length()) {
            n++;
        }
        return n;
    }

    private static String stripComments(String source) {
        String noBlock = source.replaceAll("/\\*[\\s\\S]*?\\*/", "");
        StringBuilder out = new StringBuilder(noBlock.length());
        for (String line : noBlock.split("\n", -1)) {
            int idx = line.indexOf("//");
            out.append(idx >= 0 ? line.substring(0, idx) : line).append('\n');
        }
        return out.toString();
    }

    @FunctionalInterface
    private interface SourceAssertion {
        void check(String text);
    }

    static final class FakeLane implements CompareLane {
        private final String address;
        private final StreamRingBuffer buffer;

        FakeLane(String address, long start, int capacity) {
            this.address = address;
            this.buffer = new StreamRingBuffer(capacity, start);
        }

        void write(byte[] data) {
            buffer.write(Unpooled.wrappedBuffer(data));
        }

        @Override
        public String getAddress() {
            return address;
        }

        @Override
        public String getReplId() {
            return "rid";
        }

        @Override
        public long getContinueOffset() {
            return 100L;
        }

        @Override
        public StreamRingBuffer getBuffer() {
            return buffer;
        }

        @Override
        public void disconnect() {
        }

        @Override
        public void reconnect() {
        }

        @Override
        public void close() {
        }
    }
}
