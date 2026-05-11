package com.ctrip.xpipe.redis.keeper.store.gtid.index;

import com.ctrip.xpipe.redis.core.store.CommandWriter;
import com.ctrip.xpipe.redis.keeper.config.KeeperConfig;
import com.ctrip.xpipe.redis.keeper.monitor.CommandStoreDelay;
import com.ctrip.xpipe.utils.OffsetNotifier;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.util.concurrent.ScheduledFuture;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.*;

@RunWith(MockitoJUnitRunner.class)
public class TimerSlidingWindowTest {

    @Mock
    private KeeperConfig keeperConfig;
    @Mock
    private CommandWriter commandWriter;
    @Mock
    private CommandStoreDelay commandStoreDelay;
    @Mock
    private OffsetNotifier offsetNotifier;
    @Mock
    private NioEventLoopGroup eventLoopGroup;

    private TimerSlidingWindow window;
    private final AtomicLong totalLength = new AtomicLong(0);
    private final List<Integer> writeSizes = new ArrayList<>();
    private final List<Runnable> scheduledTasks = new ArrayList<>();
    private final List<ScheduledFuture<?>> scheduledFutures = new ArrayList<>();

    @Before
    public void setUp() throws Exception {
        when(keeperConfig.getCmdBatchWriteSize()).thenReturn(1024);
        when(keeperConfig.getCmdBatchFlushIntervalMillis()).thenReturn(200l);

        writeSizes.clear();
        // 持续生效的 Answer：累加 totalLength、记录写入量，不清除
        doAnswer(inv -> {
            ByteBuf buf = inv.getArgument(0);
            int size = buf.readableBytes();
            writeSizes.add(size);
            totalLength.addAndGet(size);
            return null;
        }).when(commandWriter).write(any(ByteBuf.class));

        when(commandWriter.totalLength()).thenAnswer(inv -> totalLength.get());

        scheduledTasks.clear();
        scheduledFutures.clear();
        when(eventLoopGroup.schedule(any(Runnable.class), anyLong(), any(TimeUnit.class)))
                .thenAnswer(inv -> {
                    Runnable task = inv.getArgument(0);
                    scheduledTasks.add(task);
                    ScheduledFuture<?> future = mock(ScheduledFuture.class);
                    when(future.isDone()).thenReturn(false);
                    scheduledFutures.add(future);
                    return future;
                });

        window = new TimerSlidingWindow(keeperConfig, commandWriter,
                commandStoreDelay, offsetNotifier, eventLoopGroup);
    }

    @After
    public void tearDown() throws Exception {
        if (window != null) {
            window.close();
        }
    }

    @Test
    public void testFirstWriteLowRateDirectFlush() throws IOException {
        ByteBuf data = Unpooled.wrappedBuffer(new byte[100]);
        window.write(data);

        verify(commandStoreDelay).beginWrite();
        verify(commandWriter).write(data);
        long offset = totalLength.get() - 1;
        verify(commandStoreDelay).endWrite(offset);
        verify(offsetNotifier).offsetIncreased(offset);
        assertTrue(scheduledTasks.isEmpty());
    }

    @Test
    public void testHighRateBufferAccumulation() throws IOException, InterruptedException {
        ByteBuf firstData = Unpooled.wrappedBuffer(new byte[524288]);
        window.write(firstData);

        // 清除交互记录，但保留存根（避免 reset）
        clearInvocations(commandWriter, commandStoreDelay, offsetNotifier);
        writeSizes.clear();           // 清空本地记录，便于后续断言
        Thread.sleep(2);

        ByteBuf secondData = Unpooled.wrappedBuffer(new byte[100]);
        window.write(secondData);

        verify(commandWriter, never()).write(any(ByteBuf.class));
        verify(commandStoreDelay, never()).beginWrite();
        assertEquals(1, scheduledTasks.size());
    }

    @Test
    public void testHighRateFlushOnSizeThreshold() throws IOException, InterruptedException {
        when(keeperConfig.getCmdBatchWriteSize()).thenReturn(200);

        ByteBuf firstData = Unpooled.wrappedBuffer(new byte[524288]);
        window.write(firstData);

        clearInvocations(commandWriter, commandStoreDelay, offsetNotifier);
        writeSizes.clear();
        Thread.sleep(2);

        ByteBuf secondData = Unpooled.wrappedBuffer(new byte[250]);
        window.write(secondData);

        verify(commandStoreDelay).beginWrite();
        verify(commandWriter).write(any(ByteBuf.class));
        long offset = totalLength.get() - 1;          // 此时 totalLength 已包含 250 字节
        verify(commandStoreDelay).endWrite(offset);
        verify(offsetNotifier).offsetIncreased(offset);
        assertEquals(Collections.singletonList(250), writeSizes);
        verify(scheduledFutures.get(0)).cancel(false);
    }

    @Test
    public void testHighRateFlushOnMaxStayTime() throws IOException, InterruptedException {
        when(keeperConfig.getCmdBatchFlushIntervalMillis()).thenReturn(100l);

        ByteBuf firstData = Unpooled.wrappedBuffer(new byte[524288]);
        window.write(firstData);

        clearInvocations(commandWriter, commandStoreDelay, offsetNotifier);
        writeSizes.clear();
        Thread.sleep(2);

        ByteBuf secondData = Unpooled.wrappedBuffer(new byte[50]);
        window.write(secondData);

        assertEquals(1, scheduledTasks.size());
        verify(commandWriter, never()).write(any(ByteBuf.class));

        scheduledTasks.get(0).run();                 // 手动触发定时任务

        verify(commandStoreDelay).beginWrite();
        verify(commandWriter).write(any(ByteBuf.class));
        long offset = totalLength.get() - 1;
        verify(commandStoreDelay).endWrite(offset);
        verify(offsetNotifier).offsetIncreased(offset);
        assertEquals(Collections.singletonList(50), writeSizes);
        verify(scheduledFutures.get(0)).cancel(false);
    }

    /**
     * 高速率将数据聚合在窗口中，随后强制将速率置为 0（模拟衰减），
     * 下一次写入应先将窗口内容刷盘，再直接写入新数据。
     */
    @Test
    public void testRateDecayToLowThenFlushBufferAndSingle() throws Exception {
        // 构建窗口中有 100 字节的场景
        ByteBuf firstData = Unpooled.wrappedBuffer(new byte[524288]);
        window.write(firstData);
        Thread.sleep(2);
        ByteBuf secondData = Unpooled.wrappedBuffer(new byte[100]);
        window.write(secondData);

        // 通过反射将内部速率估计值强制设为 0，模拟速率衰减
        setInternalRateToZero();

        clearInvocations(commandWriter, commandStoreDelay, offsetNotifier);
        writeSizes.clear();

        ByteBuf thirdData = Unpooled.wrappedBuffer(new byte[10]);
        window.write(thirdData);

        // 预期：先刷窗口（100 字节），再直接写第三条（10 字节）
        verify(commandWriter, times(2)).write(any(ByteBuf.class));
        verify(commandStoreDelay, times(2)).beginWrite();
        assertEquals(2, writeSizes.size());
        assertEquals(Integer.valueOf(100), writeSizes.get(0));
        assertEquals(Integer.valueOf(10), writeSizes.get(1));
        verify(offsetNotifier, times(2)).offsetIncreased(anyLong());
    }

    @Test
    public void testFlushAll() throws IOException, InterruptedException {
        ByteBuf firstData = Unpooled.wrappedBuffer(new byte[524288]);
        window.write(firstData);
        Thread.sleep(2);
        ByteBuf secondData = Unpooled.wrappedBuffer(new byte[200]);
        window.write(secondData);

        clearInvocations(commandWriter, commandStoreDelay, offsetNotifier);
        writeSizes.clear();

        window.flushAll();

        verify(commandStoreDelay).beginWrite();
        verify(commandWriter).write(any(ByteBuf.class));
        assertEquals(Collections.singletonList(200), writeSizes);
        long offset = totalLength.get() - 1;
        verify(commandStoreDelay).endWrite(offset);
        verify(offsetNotifier).offsetIncreased(offset);
    }

    @Test
    public void testFlushAllEmptyWindow() throws IOException {
        clearInvocations(commandWriter);
        window.flushAll();
        verify(commandWriter, never()).write(any(ByteBuf.class));
    }

    @Test
    public void testClose() throws IOException, InterruptedException {
        ByteBuf firstData = Unpooled.wrappedBuffer(new byte[524288]);
        window.write(firstData);
        Thread.sleep(2);
        ByteBuf secondData = Unpooled.wrappedBuffer(new byte[300]);
        window.write(secondData);

        clearInvocations(commandWriter, commandStoreDelay, offsetNotifier);
        writeSizes.clear();

        window.close();

        verify(commandStoreDelay).beginWrite();
        verify(commandWriter).write(any(ByteBuf.class));
        assertEquals(Collections.singletonList(300), writeSizes);
        long offset = totalLength.get() - 1;
        verify(commandStoreDelay).endWrite(offset);
        verify(offsetNotifier).offsetIncreased(offset);
    }

    // ==================== 辅助方法 ====================

    /**
     * 通过反射将 TimerSlidingWindow 内部 RateEstimator 的 estimatedRate 设为 0，
     * 同时更新 lastUpdateTime，使后续 getRate() 直接返回 0，触发低速率分支。
     */
    private void setInternalRateToZero() throws Exception {
        Field rateEstimatorField = TimerSlidingWindow.class.getDeclaredField("rateEstimator");
        rateEstimatorField.setAccessible(true);
        Object rateEstimator = rateEstimatorField.get(window);

        Field estimatedRateField = rateEstimator.getClass().getDeclaredField("estimatedRate");
        estimatedRateField.setAccessible(true);
        estimatedRateField.set(rateEstimator, 0.0);

        Field lastUpdateTimeField = rateEstimator.getClass().getDeclaredField("lastUpdateTime");
        lastUpdateTimeField.setAccessible(true);
        lastUpdateTimeField.set(rateEstimator, System.currentTimeMillis());
    }
}