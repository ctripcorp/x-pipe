package com.ctrip.xpipe.redis.comparator.stream;

import com.ctrip.xpipe.AbstractTest;
import com.ctrip.xpipe.api.monitor.EventMonitor;
import com.ctrip.xpipe.command.DefaultCommandFuture;
import com.ctrip.xpipe.endpoint.DefaultEndPoint;
import com.ctrip.xpipe.netty.commands.ByteBufReceiver;
import com.ctrip.xpipe.redis.comparator.config.ComparatorConfig;
import com.ctrip.xpipe.redis.comparator.stream.StreamRingBuffer.PeekStatus;
import com.ctrip.xpipe.redis.core.protocal.cmd.CmdTailGapAllowedSync;
import io.netty.buffer.Unpooled;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class KeeperReplStreamTest extends AbstractTest {

    private static final String REPL_ID = "0123456789012345678901234567890123456789";

    private static final String SOURCE = "src/main/java/com/ctrip/xpipe/redis/comparator/stream/KeeperReplStream.java";

    private final List<KeeperReplStream> streams = new ArrayList<>();

    @After
    public void afterKeeperReplStreamTest() {
        for (KeeperReplStream stream : streams) {
            stream.stop();
        }
        streams.clear();
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
        Assert.assertFalse(text.contains("connectGen"));
        Assert.assertFalse(text.contains("cacheEpoch"));
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
        return stream(probe, monitor, new AtomicInteger()::incrementAndGet, scheduled);
    }

    private KeeperReplStream stream(KeeperReplStream.PrepareWatchProbe probe, EventMonitor monitor,
            AtomicInteger wakes) {
        return stream(probe, monitor, wakes::incrementAndGet, scheduled);
    }

    private KeeperReplStream stream(KeeperReplStream.PrepareWatchProbe probe, EventMonitor monitor,
            Runnable dataAvailable) {
        return stream(probe, monitor, dataAvailable, scheduled);
    }

    private KeeperReplStream stream(KeeperReplStream.PrepareWatchProbe probe, EventMonitor monitor,
            Runnable dataAvailable, ScheduledExecutorService scheduler) {
        ComparatorConfig config = new ComparatorConfig() {
            @Override
            public int getStreamBufferBytes() {
                return 64;
            }
        };
        KeeperReplStream stream = new KeeperReplStream(
                new DefaultEndPoint("127.0.0.1", randomPort()),
                scheduler, config, dataAvailable, probe, false, monitor);
        stream.setReconnectDelayMilli(1);
        streams.add(stream);
        return stream;
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

        CaptureDelayedSchedule() {
            super(1);
        }

        @Override
        public ScheduledFuture<?> schedule(Runnable command, long delay, TimeUnit unit) {
            delayed = command;
            return super.schedule(() -> { }, 1, TimeUnit.DAYS);
        }
    }
}
