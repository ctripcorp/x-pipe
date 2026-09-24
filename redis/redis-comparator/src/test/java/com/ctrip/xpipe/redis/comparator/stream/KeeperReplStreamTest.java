package com.ctrip.xpipe.redis.comparator.stream;

import com.ctrip.xpipe.AbstractTest;
import com.ctrip.xpipe.api.command.CommandFuture;
import com.ctrip.xpipe.api.monitor.EventMonitor;
import com.ctrip.xpipe.command.DefaultCommandFuture;
import com.ctrip.xpipe.endpoint.DefaultEndPoint;
import com.ctrip.xpipe.netty.commands.ByteBufReceiver;
import com.ctrip.xpipe.redis.comparator.compare.ShardComparator;
import com.ctrip.xpipe.simpleserver.AbstractIoAction;
import com.ctrip.xpipe.simpleserver.Server;
import com.ctrip.xpipe.utils.XpipeThreadFactory;
import com.ctrip.xpipe.redis.comparator.compare.ShardComparator.CompareOnceResult;
import com.ctrip.xpipe.redis.comparator.config.ComparatorConfig;
import com.ctrip.xpipe.redis.comparator.config.ComparatorConstants;
import com.ctrip.xpipe.redis.comparator.report.CompareReporter;
import com.ctrip.xpipe.redis.comparator.report.CompareReporter.MismatchReport;
import com.ctrip.xpipe.redis.comparator.stream.StreamRingBuffer.PeekStatus;
import com.ctrip.xpipe.redis.core.protocal.cmd.CmdTailGapAllowedSync;
import io.netty.buffer.Unpooled;
import io.netty.channel.EventLoop;
import io.netty.channel.nio.NioEventLoopGroup;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class KeeperReplStreamTest extends AbstractTest {

    private static final String REPL_ID = "0123456789012345678901234567890123456789";

    private static final String SOURCE = "src/main/java/com/ctrip/xpipe/redis/comparator/stream/KeeperReplStream.java";

    private final List<KeeperReplStream> streams = new ArrayList<>();

    private NioEventLoopGroup ioGroup;

    private EventLoop eventLoop;

    @Before
    public void beforeKeeperReplStreamTest() {
        ioGroup = new NioEventLoopGroup(1, XpipeThreadFactory.create("keeper-repl-stream-test"));
        eventLoop = ioGroup.next();
    }

    @After
    public void afterKeeperReplStreamTest() {
        for (KeeperReplStream stream : streams) {
            stream.stop();
        }
        streams.clear();
        eventLoop = null;
        if (ioGroup != null) {
            ioGroup.shutdownGracefully(0, 200, TimeUnit.MILLISECONDS);
            ioGroup = null;
        }
    }

    @Test
    public void testPrepareWatchOffDoesNotOpenStreamAndRetriesWithoutAffectingPeer() throws Exception {
        AtomicInteger offProbes = new AtomicInteger();
        AtomicInteger onProbes = new AtomicInteger();
        KeeperReplStream off = stream(countingProbe(offProbes, false), new RecordingMonitor());
        KeeperReplStream on = stream(countingProbe(onProbes, true), new RecordingMonitor());
        off.start();
        on.start();

        Assert.assertNull(off.currentSync());
        Assert.assertNull(off.getReplId());
        Assert.assertNotNull(on.currentSync());
        CmdTailGapAllowedSync firstOn = on.currentSync();
        Assert.assertEquals(1, offProbes.get());

        TimeUnit.MILLISECONDS.sleep(50);
        Assert.assertEquals(1, offProbes.get());
        Assert.assertSame(firstOn, on.currentSync());
        Assert.assertEquals(1, onProbes.get());

        off.invalidatePrepareWatchCache();
        waitConditionUntilTimeOut(() -> offProbes.get() >= 2, 2000);
        Assert.assertNull(off.currentSync());
        Assert.assertSame(firstOn, on.currentSync());
        Assert.assertEquals(1, onProbes.get());
    }

    @Test
    public void testContinueOffsetUsedAsIsAndRingBufferMatchesPayload() {
        AtomicInteger wakes = new AtomicInteger();
        KeeperReplStream stream = stream(fixedProbe(true), new RecordingMonitor(), wakes);
        stream.start();
        CmdTailGapAllowedSync sync = stream.currentSync();
        Assert.assertNotNull(sync);
        sync.getRequest().release();

        byte[] payload = new byte[]{0x00, 0x01, (byte) 0xff, 'a', 'b', 'c'};
        Assert.assertEquals(ByteBufReceiver.RECEIVER_RESULT.CONTINUE, feedContinue(sync, 100L));
        Assert.assertEquals(REPL_ID, stream.getReplId());
        Assert.assertEquals(100L, stream.getContinueOffset());
        Assert.assertEquals(100L, stream.getBuffer().getStreamStart());
        Assert.assertEquals(100L, stream.getBuffer().getReceivedEnd());

        Assert.assertEquals(ByteBufReceiver.RECEIVER_RESULT.CONTINUE,
                sync.receive(null, Unpooled.wrappedBuffer(payload)));
        Assert.assertEquals(100L + payload.length, stream.getBuffer().getReceivedEnd());
        byte[] reuse = new byte[payload.length];
        Assert.assertEquals(PeekStatus.HIT, stream.getBuffer().peek(100L, payload.length, reuse));
        Assert.assertArrayEquals(payload, reuse);
        Assert.assertTrue(wakes.get() >= 2);
    }

    @Test
    public void testFullResyncDoesNotTakeRdbAndRetries() throws Exception {
        RecordingMonitor monitor = new RecordingMonitor();
        KeeperReplStream stream = stream(fixedProbe(true), monitor);
        stream.start();
        CmdTailGapAllowedSync first = stream.currentSync();
        Assert.assertNotNull(first);

        ByteBufReceiver.RECEIVER_RESULT result = first.receive(null, Unpooled.wrappedBuffer(
                ("+FULLRESYNC " + REPL_ID + " 1\r\n").getBytes(StandardCharsets.UTF_8)));
        Assert.assertEquals(ByteBufReceiver.RECEIVER_RESULT.FAIL, result);
        Assert.assertTrue(first.future().isDone());
        Assert.assertFalse(first.future().isSuccess());
        Assert.assertNull(stream.getReplId());

        waitConditionUntilTimeOut(() -> stream.currentSync() != null && stream.currentSync() != first, 2000);
        Assert.assertNotSame(first, stream.currentSync());
        waitConditionUntilTimeOut(() -> monitor.events.contains(
                KeeperReplStream.MONITOR_TYPE + "/" + KeeperReplStream.FULL_RESYNC), 2000);

        String text = source();
        Assert.assertFalse(text.contains("ReplicationStore"));
        Assert.assertFalse(text.contains(".createRdbReader("));
        Assert.assertFalse(text.contains("RdbDumper"));
        Assert.assertTrue(text.contains("ConfigGetPrepareWatch"));
        Assert.assertTrue(text.contains("RdbRejectedException"));
        Assert.assertTrue(text.contains("FixedObjectPool"));
        Assert.assertFalse(text.contains("XpipeNettyClientKeyedObjectPool"));
        Assert.assertFalse(text.contains("NettyClientFactory("));
        Assert.assertFalse(text.contains("new NioEventLoopGroup"));
        Assert.assertTrue(text.contains("EventLoop eventLoop"));
        Assert.assertTrue(text.contains("CONNECT_TIMEOUT_MILLIS"));
        Assert.assertFalse(text.contains("connectGen"));
        Assert.assertFalse(text.contains("cacheEpoch"));
        Assert.assertTrue(text.contains("ReplConfType.LISTENING_PORT"));
        Assert.assertTrue(text.contains("ReplConfType.ACK"));
        Assert.assertFalse(text.contains("ReplConfType.CAPA"));
        Assert.assertFalse(text.contains("getComparedEnd"));
        Assert.assertTrue(text.contains("getReceivedEnd()"));
        Assert.assertTrue(text.contains("STREAM_RECONNECT_MAX_MILLI"));
        Assert.assertTrue(text.contains("scheduleWithFixedDelay"));
    }

    @Test
    public void testRealReplconfOnSameConnectionWithoutProbe() throws Exception {
        List<String> commands = new CopyOnWriteArrayList<>();
        Server server = startServer(socket -> new AbstractIoAction(socket) {
            @Override
            protected Object doRead(InputStream ins) throws IOException {
                return readLine(ins);
            }

            @Override
            protected void doWrite(OutputStream ous, Object readResult) throws IOException {
                if (readResult == null) {
                    return;
                }
                String line = readResult.toString().trim();
                commands.add(line);
                String lower = line.toLowerCase();
                if (lower.startsWith("config get")) {
                    ous.write("*2\r\n$13\r\nprepare-watch\r\n$1\r\n1\r\n".getBytes(StandardCharsets.US_ASCII));
                } else if (lower.startsWith("replconf listening-port")) {
                    ous.write("+OK\r\n".getBytes(StandardCharsets.US_ASCII));
                } else if (lower.startsWith("psync")) {
                    ous.write(("+CONTINUE " + REPL_ID + " 100\r\n").getBytes(StandardCharsets.US_ASCII));
                    ous.write(new byte[]{1, 2, 3});
                } else if (lower.startsWith("replconf ack")) {
                    return;
                } else {
                    ous.write("+OK\r\n".getBytes(StandardCharsets.US_ASCII));
                }
                ous.flush();
            }
        });
        KeeperReplStream stream = new KeeperReplStream(
                new DefaultEndPoint("127.0.0.1", server.getPort()),
                scheduled, compareConfig(), () -> { }, "c", "s",
                KeeperReplStream.TEST_HTTP_PORT, eventLoop);
        stream.setAckIntervalMilli(5);
        streams.add(stream);
        stream.start();

        waitConditionUntilTimeOut(() -> REPL_ID.equals(stream.getReplId()), 3000);
        waitConditionUntilTimeOut(() -> stream.getBuffer() != null
                && stream.getBuffer().getReceivedEnd() == 103L, 2000);
        waitConditionUntilTimeOut(() -> commands.stream().anyMatch(c ->
                c.toLowerCase().startsWith("replconf ack") && c.contains("103")), 2000);

        Assert.assertEquals(1, countStartsWith(commands, "replconf listening-port"));
        Assert.assertTrue(commands.stream().anyMatch(c ->
                c.toLowerCase().contains("listening-port " + KeeperReplStream.TEST_HTTP_PORT)));
        Assert.assertEquals(1, countStartsWith(commands, "config get"));
        Assert.assertEquals(1, countStartsWith(commands, "psync"));
        Assert.assertTrue(commands.stream().anyMatch(c ->
                c.toLowerCase().startsWith("psync") && c.contains("-4")));
        int configAt = indexStartsWith(commands, "config get");
        int listenAt = indexStartsWith(commands, "replconf listening-port");
        int psyncAt = indexStartsWith(commands, "psync");
        Assert.assertTrue(configAt < listenAt && listenAt < psyncAt);
        Assert.assertEquals(1, server.getTotalConnected());
        Assert.assertFalse(commands.stream().anyMatch(c -> c.toLowerCase().contains("capa")));
    }

    @Test
    public void testListeningPortBeforeStreamAndPeriodicAck() throws Exception {
        RecordingReplconf repl = new RecordingReplconf();
        KeeperReplStream stream = stream(fixedProbe(true), new RecordingMonitor(),
                new AtomicInteger()::incrementAndGet, scheduled, repl, true);
        stream.start();
        Assert.assertEquals(Collections.singletonList(KeeperReplStream.TEST_HTTP_PORT), repl.listeningPorts);
        CmdTailGapAllowedSync sync = stream.currentSync();
        Assert.assertNotNull(sync);
        sync.getRequest().release();

        feedContinue(sync, 100L);
        waitConditionUntilTimeOut(() -> !repl.acks.isEmpty(), 2000);
        Assert.assertEquals(Long.valueOf(100L), repl.acks.get(0));

        Assert.assertEquals(ByteBufReceiver.RECEIVER_RESULT.CONTINUE,
                sync.receive(null, Unpooled.wrappedBuffer(new byte[]{1, 2, 3})));
        waitConditionUntilTimeOut(() -> repl.acks.contains(103L), 2000);
        Assert.assertEquals(1, repl.listeningPorts.size());
    }

    @Test
    public void testAckFollowsReceivedEndNotComparedEndWhenPeerLags() throws Exception {
        RecordingReplconf fastRepl = new RecordingReplconf();
        RecordingReplconf slowRepl = new RecordingReplconf();
        KeeperReplStream fast = stream(fixedProbe(true), new RecordingMonitor(),
                new AtomicInteger()::incrementAndGet, scheduled, fastRepl, true);
        KeeperReplStream slow = stream(fixedProbe(true), new RecordingMonitor(),
                new AtomicInteger()::incrementAndGet, scheduled, slowRepl, true);
        fast.start();
        slow.start();
        CmdTailGapAllowedSync fastSync = fast.currentSync();
        CmdTailGapAllowedSync slowSync = slow.currentSync();
        fastSync.getRequest().release();
        slowSync.getRequest().release();
        feedContinue(fastSync, 100L);
        feedContinue(slowSync, 100L);

        byte[] prefix = {1, 2, 3, 4, 5};
        byte[] extra = new byte[40];
        Arrays.fill(extra, (byte) 9);
        fastSync.receive(null, Unpooled.wrappedBuffer(concat(prefix, extra)));
        slowSync.receive(null, Unpooled.wrappedBuffer(prefix));

        ShardComparator cmp = new ShardComparator("c1", "s1", Arrays.asList(fast, slow),
                compareConfig(), new NoopReporter());
        Assert.assertTrue(cmp.alignStart());
        CompareOnceResult result;
        int steps = 0;
        do {
            result = cmp.compareOnce();
            Assert.assertTrue(++steps < 100);
        } while (result != CompareOnceResult.NO_DATA);
        Assert.assertEquals(105L, cmp.getComparedEnd());
        Assert.assertEquals(145L, fast.getBuffer().getReceivedEnd());
        waitConditionUntilTimeOut(() -> fastRepl.acks.contains(145L), 2000);
        Assert.assertEquals(145L, last(fastRepl.acks).longValue());
        Assert.assertNotEquals(cmp.getComparedEnd(), last(fastRepl.acks).longValue());
    }

    @Test
    public void testReconnectBackoffStaysWithinBoundsAndUsesNewInstance() {
        CaptureDelayedSchedule manual = new CaptureDelayedSchedule();
        try {
            KeeperReplStream stream = stream(fixedProbe(true), new RecordingMonitor(),
                    new AtomicInteger()::incrementAndGet, manual, new RecordingReplconf(), false);
            stream.start();
            CmdTailGapAllowedSync prev = stream.currentSync();
            Assert.assertNotNull(prev);
            for (int i = 0; i < 7; i++) {
                prev.receive(null, Unpooled.wrappedBuffer(
                        ("+FULLRESYNC " + REPL_ID + " 1\r\n").getBytes(StandardCharsets.UTF_8)));
                Assert.assertNull(stream.getReplId());
                long expected = Math.min(
                        (long) ComparatorConstants.STREAM_RECONNECT_MIN_MILLI << i,
                        ComparatorConstants.STREAM_RECONNECT_MAX_MILLI);
                Assert.assertEquals(expected, manual.delays.get(i).longValue());
                Assert.assertNotNull(manual.delayed);
                manual.delayed.run();
                CmdTailGapAllowedSync neu = stream.currentSync();
                Assert.assertNotNull(neu);
                Assert.assertNotSame(prev, neu);
                prev = neu;
            }
            Assert.assertEquals(7, stream.getStreamReconnectCount());
        } finally {
            manual.shutdownNow();
        }
    }

    @Test
    public void testDisconnectCancelsPendingReconnect() {
        CaptureDelayedSchedule manual = new CaptureDelayedSchedule();
        try {
            KeeperReplStream stream = stream(fixedProbe(true), new RecordingMonitor(),
                    new AtomicInteger()::incrementAndGet, manual, new RecordingReplconf(), false);
            stream.start();
            CmdTailGapAllowedSync first = stream.currentSync();
            first.receive(null, Unpooled.wrappedBuffer(
                    ("+FULLRESYNC " + REPL_ID + " 1\r\n").getBytes(StandardCharsets.UTF_8)));
            Assert.assertNotNull(manual.delayed);
            stream.disconnect();
            Assert.assertNull(stream.getReplId());
            manual.delayed.run();
            Assert.assertNull(stream.currentSync());
            Assert.assertEquals(1, stream.getStreamReconnectCount());
        } finally {
            manual.shutdownNow();
        }
    }

    @Test
    public void testAckThrowDoesNotFailPsync() throws Exception {
        RecordingReplconf repl = new RecordingReplconf();
        repl.ackThrow = new RuntimeException("ack");
        KeeperReplStream stream = stream(fixedProbe(true), new RecordingMonitor(),
                new AtomicInteger()::incrementAndGet, scheduled, repl, true);
        stream.start();
        CmdTailGapAllowedSync sync = stream.currentSync();
        sync.getRequest().release();
        Assert.assertEquals(ByteBufReceiver.RECEIVER_RESULT.CONTINUE, feedContinue(sync, 100L));
        TimeUnit.MILLISECONDS.sleep(30);
        Assert.assertFalse(sync.future().isDone());
        Assert.assertEquals(ByteBufReceiver.RECEIVER_RESULT.CONTINUE,
                sync.receive(null, Unpooled.wrappedBuffer(new byte[]{7, 8})));
        Assert.assertEquals(102L, stream.getBuffer().getReceivedEnd());
        Assert.assertEquals(REPL_ID, stream.getReplId());
    }

    @Test
    public void testListeningPortFailRetriesWithoutOpeningSync() throws Exception {
        RecordingReplconf repl = new RecordingReplconf();
        repl.failListening = true;
        KeeperReplStream stream = stream(fixedProbe(true), new RecordingMonitor(),
                new AtomicInteger()::incrementAndGet, scheduled, repl, true);
        stream.start();
        Assert.assertNull(stream.currentSync());
        Assert.assertFalse(repl.listeningPorts.isEmpty());
        waitConditionUntilTimeOut(() -> stream.getStreamReconnectCount() >= 1, 2000);
        Assert.assertNull(stream.currentSync());
    }

    @Test
    public void testDisconnectNullsReplIdAndReconnectInstallsNewBuffer() {
        KeeperReplStream stream = stream(fixedProbe(true), new RecordingMonitor());
        stream.start();
        CmdTailGapAllowedSync first = stream.currentSync();
        first.getRequest().release();
        feedContinue(first, 50L);
        StreamRingBuffer old = stream.getBuffer();
        first.receive(null, Unpooled.wrappedBuffer(new byte[]{1, 2, 3}));
        Assert.assertEquals(53L, old.getReceivedEnd());

        stream.disconnect();
        Assert.assertNull(stream.getReplId());
        Assert.assertNull(stream.currentSync());
        Assert.assertSame(old, stream.getBuffer());
        Assert.assertEquals(53L, old.getReceivedEnd());

        stream.reconnect();
        CmdTailGapAllowedSync neu = stream.currentSync();
        Assert.assertNotSame(first, neu);
        neu.getRequest().release();
        feedContinue(neu, 200L);
        Assert.assertEquals(REPL_ID, stream.getReplId());
        Assert.assertNotSame(old, stream.getBuffer());
        Assert.assertEquals(200L, stream.getBuffer().getStreamStart());
        Assert.assertEquals(200L, stream.getContinueOffset());
    }

    @Test
    public void testReconnectAbandonsOldSessionWithoutPriorDisconnect() {
        KeeperReplStream stream = stream(fixedProbe(true), new RecordingMonitor());
        stream.start();
        CmdTailGapAllowedSync first = stream.currentSync();
        first.getRequest().release();
        feedContinue(first, 50L);
        StreamRingBuffer old = stream.getBuffer();
        first.receive(null, Unpooled.wrappedBuffer(new byte[]{1, 2, 3}));

        stream.reconnect();
        Assert.assertNull(stream.getReplId());
        CmdTailGapAllowedSync neu = stream.currentSync();
        Assert.assertNotSame(first, neu);
        Assert.assertTrue(first.future().isDone());

        Assert.assertEquals(ByteBufReceiver.RECEIVER_RESULT.ALREADY_FINISH,
                first.receive(null, Unpooled.wrappedBuffer(new byte[]{4, 5})));
        Assert.assertEquals(53L, old.getReceivedEnd());

        neu.getRequest().release();
        feedContinue(neu, 200L);
        Assert.assertEquals(REPL_ID, stream.getReplId());
        Assert.assertNotSame(old, stream.getBuffer());
        Assert.assertEquals(200L, stream.getBuffer().getReceivedEnd());
        Assert.assertEquals(53L, old.getReceivedEnd());
    }

    @Test
    public void testLateContinueAfterDisconnectDoesNotRestoreReplId() {
        KeeperReplStream stream = stream(fixedProbe(true), new RecordingMonitor());
        stream.start();
        CmdTailGapAllowedSync first = stream.currentSync();
        first.getRequest().release();
        stream.disconnect();
        Assert.assertNull(stream.getReplId());
        Assert.assertNull(stream.currentSync());

        Assert.assertEquals(ByteBufReceiver.RECEIVER_RESULT.ALREADY_FINISH, feedContinue(first, 50L));
        Assert.assertNull(stream.getReplId());
        Assert.assertNull(stream.getBuffer());
    }

    @Test
    public void testAbandonedSessionCommandsWriteOldBufferOnly() {
        KeeperReplStream stream = stream(fixedProbe(true), new RecordingMonitor());
        stream.start();
        CmdTailGapAllowedSync first = stream.currentSync();
        first.getRequest().release();
        feedContinue(first, 50L);
        StreamRingBuffer old = stream.getBuffer();
        first.receive(null, Unpooled.wrappedBuffer(new byte[]{1, 2, 3}));

        stream.disconnect();
        Assert.assertEquals(ByteBufReceiver.RECEIVER_RESULT.ALREADY_FINISH,
                first.receive(null, Unpooled.wrappedBuffer(new byte[]{9, 9})));
        Assert.assertEquals(53L, old.getReceivedEnd());
        Assert.assertNull(stream.getReplId());
    }

    @Test
    public void testDataAvailableThrowDoesNotFailPsync() {
        AtomicInteger wakes = new AtomicInteger();
        KeeperReplStream stream = stream(fixedProbe(true), new RecordingMonitor(), () -> {
            wakes.incrementAndGet();
            throw new RuntimeException("wake");
        });
        stream.start();
        CmdTailGapAllowedSync sync = stream.currentSync();
        sync.getRequest().release();

        Assert.assertEquals(ByteBufReceiver.RECEIVER_RESULT.CONTINUE, feedContinue(sync, 100L));
        Assert.assertEquals(REPL_ID, stream.getReplId());
        byte[] payload = new byte[]{7, 8, 9};
        Assert.assertEquals(ByteBufReceiver.RECEIVER_RESULT.CONTINUE,
                sync.receive(null, Unpooled.wrappedBuffer(payload)));
        Assert.assertEquals(103L, stream.getBuffer().getReceivedEnd());
        Assert.assertTrue(wakes.get() >= 2);
        Assert.assertFalse(sync.future().isDone());
    }

    @Test
    public void testEventMonitorThrowStillReconnects() throws Exception {
        ThrowingMonitor monitor = new ThrowingMonitor();
        KeeperReplStream stream = stream(fixedProbe(true), monitor);
        stream.start();
        CmdTailGapAllowedSync first = stream.currentSync();
        first.receive(null, Unpooled.wrappedBuffer(
                ("+FULLRESYNC " + REPL_ID + " 1\r\n").getBytes(StandardCharsets.UTF_8)));
        Assert.assertNull(stream.getReplId());
        waitConditionUntilTimeOut(() -> stream.currentSync() != null && stream.currentSync() != first, 2000);
    }

    @Test
    public void testStartedReconnectTimerDoesNotOverrideFreshStream() {
        CaptureDelayedSchedule manual = new CaptureDelayedSchedule();
        try {
            KeeperReplStream stream = stream(fixedProbe(true), new RecordingMonitor(),
                    new AtomicInteger()::incrementAndGet, manual);
            stream.setReconnectDelayMilli(1);
            stream.start();
            CmdTailGapAllowedSync first = stream.currentSync();
            Assert.assertNotNull(first);
            first.receive(null, Unpooled.wrappedBuffer(
                    ("+FULLRESYNC " + REPL_ID + " 1\r\n").getBytes(StandardCharsets.UTF_8)));
            Assert.assertNotNull(manual.delayed);
            stream.reconnect();
            CmdTailGapAllowedSync neu = stream.currentSync();
            Assert.assertNotNull(neu);
            Assert.assertNotSame(first, neu);
            manual.delayed.run();
            Assert.assertSame(neu, stream.currentSync());
            Assert.assertFalse(neu.future().isDone());
        } finally {
            manual.shutdownNow();
        }
    }

    @Test
    public void testStalePrepareWatchFailDoesNotKillNextGeneration() throws Exception {
        PendingProbe probe = new PendingProbe();
        RecordingMonitor monitor = new RecordingMonitor();
        KeeperReplStream stream = stream(probe, monitor);
        stream.start();
        Assert.assertEquals(1, probe.issued.size());
        Assert.assertNull(stream.currentSync());

        stream.reconnect();
        Assert.assertEquals(2, probe.issued.size());

        probe.issued.get(0).setFailure(new RuntimeException("stale prepare-watch"));
        TimeUnit.MILLISECONDS.sleep(30);
        Assert.assertNull(stream.currentSync());
        Assert.assertEquals(2, probe.issued.size());
        Assert.assertFalse(monitor.events.contains(
                KeeperReplStream.MONITOR_TYPE + "/" + KeeperReplStream.STREAM_FAIL));

        probe.issued.get(1).setSuccess(true);
        Assert.assertNotNull(stream.currentSync());
        CmdTailGapAllowedSync live = stream.currentSync();
        TimeUnit.MILLISECONDS.sleep(30);
        Assert.assertSame(live, stream.currentSync());
        Assert.assertEquals(2, probe.issued.size());
        Assert.assertFalse(live.future().isDone());
    }

    @Test
    public void testOnCommandsPathHasNoCompareDumpMetricOrWait() throws Exception {
        String text = source();
        int start = text.indexOf("public void onCommands(ByteBuf buf)");
        Assert.assertTrue(start > 0);
        int end = text.indexOf("public void onKeeperContinue", start);
        Assert.assertTrue(end > start);
        String onCommands = text.substring(start, end);
        Assert.assertTrue(onCommands.contains("target.write(buf)"));
        Assert.assertTrue(onCommands.contains("wakeCompareThread()"));
        Assert.assertFalse(onCommands.contains("compareOnce"));
        Assert.assertFalse(onCommands.contains("ShardComparator"));
        Assert.assertFalse(onCommands.contains("MetricProxy"));
        Assert.assertFalse(onCommands.contains("hex"));
        Assert.assertFalse(onCommands.contains("HexDump"));
        Assert.assertFalse(onCommands.contains("EventMonitor"));
        Assert.assertFalse(onCommands.contains("Thread.sleep"));
        Assert.assertFalse(onCommands.contains("CountDownLatch"));
        Assert.assertFalse(onCommands.contains(".await("));
        Assert.assertFalse(onCommands.contains("wait("));
        Assert.assertTrue(text.contains("scheduled.execute"));
        Assert.assertFalse(onCommands.contains("logEvent"));
    }

    private KeeperReplStream stream(KeeperReplStream.PrepareWatchProbe probe, EventMonitor monitor) {
        return stream(probe, monitor, new AtomicInteger()::incrementAndGet, scheduled, new RecordingReplconf(), true);
    }

    private KeeperReplStream stream(KeeperReplStream.PrepareWatchProbe probe, EventMonitor monitor,
            AtomicInteger wakes) {
        return stream(probe, monitor, wakes::incrementAndGet, scheduled, new RecordingReplconf(), true);
    }

    private KeeperReplStream stream(KeeperReplStream.PrepareWatchProbe probe, EventMonitor monitor,
            Runnable dataAvailable) {
        return stream(probe, monitor, dataAvailable, scheduled, new RecordingReplconf(), true);
    }

    private KeeperReplStream stream(KeeperReplStream.PrepareWatchProbe probe, EventMonitor monitor,
            Runnable dataAvailable, ScheduledExecutorService scheduler) {
        return stream(probe, monitor, dataAvailable, scheduler, new RecordingReplconf(), true);
    }

    private KeeperReplStream stream(KeeperReplStream.PrepareWatchProbe probe, EventMonitor monitor,
            Runnable dataAvailable, ScheduledExecutorService scheduler,
            KeeperReplStream.ReplconfProbe replconf, boolean fastReconnect) {
        KeeperReplStream stream = new KeeperReplStream(
                new DefaultEndPoint("127.0.0.1", randomPort()),
                scheduler, compareConfig(), dataAvailable, eventLoop, probe, false, monitor, replconf);
        if (fastReconnect) {
            stream.setReconnectDelayMilli(1);
        }
        stream.setAckIntervalMilli(5);
        streams.add(stream);
        return stream;
    }

    private static ComparatorConfig compareConfig() {
        return new ComparatorConfig() {
            @Override
            public int getStreamBufferBytes() {
                return 64;
            }

            @Override
            public int getCompareChunkBytes() {
                return 8;
            }
        };
    }

    private static KeeperReplStream.PrepareWatchProbe fixedProbe(boolean enabled) {
        return countingProbe(new AtomicInteger(), enabled);
    }

    private static KeeperReplStream.PrepareWatchProbe countingProbe(AtomicInteger count, boolean enabled) {
        return () -> {
            count.incrementAndGet();
            DefaultCommandFuture<Boolean> future = new DefaultCommandFuture<>();
            future.setSuccess(enabled);
            return future;
        };
    }

    private static ByteBufReceiver.RECEIVER_RESULT feedContinue(CmdTailGapAllowedSync sync, long offset) {
        return sync.receive(null, Unpooled.wrappedBuffer(
                ("+CONTINUE " + REPL_ID + " " + offset + "\r\n").getBytes(StandardCharsets.UTF_8)));
    }

    private static byte[] concat(byte[] a, byte[] b) {
        byte[] out = new byte[a.length + b.length];
        System.arraycopy(a, 0, out, 0, a.length);
        System.arraycopy(b, 0, out, a.length, b.length);
        return out;
    }

    private static Long last(List<Long> values) {
        return values.get(values.size() - 1);
    }

    private static int countStartsWith(List<String> commands, String prefix) {
        String needle = prefix.toLowerCase();
        int n = 0;
        for (String command : commands) {
            if (command.toLowerCase().startsWith(needle)) {
                n++;
            }
        }
        return n;
    }

    private static int indexStartsWith(List<String> commands, String prefix) {
        String needle = prefix.toLowerCase();
        for (int i = 0; i < commands.size(); i++) {
            if (commands.get(i).toLowerCase().startsWith(needle)) {
                return i;
            }
        }
        return -1;
    }

    private static String source() throws Exception {
        return new String(Files.readAllBytes(Paths.get(SOURCE)), StandardCharsets.UTF_8);
    }

    static class RecordingMonitor implements EventMonitor {
        final List<String> events = new ArrayList<>();

        void add(String type, String name) { events.add(type + "/" + name); }

        @Override public void logEvent(String type, String name, long count) { add(type, name); }
        @Override public void logEvent(String type, String name) { add(type, name); }
        @Override public void logEvent(String type, String name, Map<String, String> p) { add(type, name); }
        @Override public void logError(String type, String name) { add(type, name); }
        @Override public void logError(String type, String name, Map<String, String> p) { add(type, name); }
        @Override public void logAlertEvent(String simpleAlertMessage) { }
    }

    static final class ThrowingMonitor extends RecordingMonitor {
        @Override
        public void logEvent(String type, String name) {
            add(type, name);
            throw new RuntimeException("monitor");
        }
    }

    static final class PendingProbe implements KeeperReplStream.PrepareWatchProbe {
        final List<DefaultCommandFuture<Boolean>> issued = new ArrayList<>();

        @Override
        public DefaultCommandFuture<Boolean> query() {
            DefaultCommandFuture<Boolean> future = new DefaultCommandFuture<>();
            issued.add(future);
            return future;
        }
    }

    static final class CaptureDelayedSchedule extends ScheduledThreadPoolExecutor {
        volatile Runnable delayed;
        final List<Long> delays = new ArrayList<>();

        CaptureDelayedSchedule() {
            super(1);
        }

        @Override
        public ScheduledFuture<?> schedule(Runnable command, long delay, TimeUnit unit) {
            long millis = unit.toMillis(delay);
            if (millis <= 0) {
                return super.schedule(command, delay, unit);
            }
            delays.add(millis);
            delayed = command;
            ScheduledFuture<?> future = super.schedule(() -> { }, 1, TimeUnit.DAYS);
            future.cancel(false);
            return future;
        }
    }

    static final class RecordingReplconf implements KeeperReplStream.ReplconfProbe {
        final List<Integer> listeningPorts = new CopyOnWriteArrayList<>();
        final List<Long> acks = new CopyOnWriteArrayList<>();
        volatile boolean failListening;
        volatile RuntimeException ackThrow;

        @Override
        public CommandFuture<Object> listeningPort(int port) {
            listeningPorts.add(port);
            DefaultCommandFuture<Object> future = new DefaultCommandFuture<>();
            if (failListening) {
                future.setFailure(new RuntimeException("listening-port"));
            } else {
                future.setSuccess("OK");
            }
            return future;
        }

        @Override
        public void ack(long receivedEnd) {
            if (ackThrow != null) {
                throw ackThrow;
            }
            acks.add(receivedEnd);
        }
    }

    static final class NoopReporter implements CompareReporter {
        @Override
        public void onMismatch(MismatchReport report) {
        }

        @Override
        public void onCompareLost(String cluster, String shard, long from, long to) {
        }

        @Override
        public void onReplIdMismatch(String cluster, String shard, List<String> replIds) {
        }
    }
}
