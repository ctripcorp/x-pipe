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
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.Assert.*;
import static org.mockito.ArgumentMatchers.*;
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
    // 通过数组控制 mock 的 getRate() 返回值
    private final double[] currentRate = {1_000_000.0}; // 默认高速
    private final AtomicLong totalLength = new AtomicLong(0);
    private final List<Integer> writeSizes = new ArrayList<>();
    private final List<Runnable> scheduledTasks = new ArrayList<>();
    private final List<ScheduledFuture<?>> scheduledFutures = new ArrayList<>();

    @Before
    public void setUp() throws Exception {
        when(keeperConfig.getCmdBatchWriteSize()).thenReturn(1024);
        when(keeperConfig.getCmdBatchFlushIntervalMillis()).thenReturn(200L);

        writeSizes.clear();
        doAnswer(inv -> {
            ByteBuf buf = inv.getArgument(0);
            int size = buf.readableBytes();
            writeSizes.add(size);
            totalLength.addAndGet(size);
            return size;
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

        // 注入可控速率的 RateEstimator mock（绕过 final 字段限制）
        Field rateField = TimerSlidingWindow.class.getDeclaredField("rateEstimator");
        rateField.setAccessible(true);
        Class<?> rateEstimatorClass = rateField.getType();
        Object mockEstimator = mock(rateEstimatorClass, invocation -> {
            if ("getRate".equals(invocation.getMethod().getName())) {
                return currentRate[0];
            }
            // update 等方法返回默认值
            return RETURNS_DEFAULTS.answer(invocation);
        });
        rateField.set(window, mockEstimator);
    }

    @After
    public void tearDown() throws Exception {
        if (window != null) {
            window.close();
        }
    }

    // 便捷设置速率
    private void setRate(double rate) {
        currentRate[0] = rate;
    }

    @Test
    public void testFirstWriteLowRateDirectFlush() throws IOException {
        setRate(100.0);
        ByteBuf data = Unpooled.wrappedBuffer(new byte[100]);

        window.write(data);

        verify(commandStoreDelay).beginWrite();
        verify(commandWriter).write(data);
        long offset = totalLength.get() - 1;
        verify(commandStoreDelay).endWrite(offset);
        verify(offsetNotifier).offsetIncreased(offset);
        assertTrue("低速率不应调度定时任务", scheduledTasks.isEmpty());
        data.release();
    }

    @Test
    public void testHighRateBufferAccumulation() throws IOException {
        setRate(1_000_000.0);
        ByteBuf data = Unpooled.wrappedBuffer(new byte[100]);

        window.write(data);

        verify(commandWriter, never()).write(any(ByteBuf.class));
        verify(commandStoreDelay, never()).beginWrite();
        assertEquals("高速率应启动一个定时任务", 1, scheduledTasks.size());
        data.release();
    }

    @Test
    public void testHighRateFlushOnSizeThreshold() throws IOException {
        when(keeperConfig.getCmdBatchWriteSize()).thenReturn(200);
        setRate(1_000_000.0);

        ByteBuf firstData = Unpooled.wrappedBuffer(new byte[150]);
        window.write(firstData);
        // 清除首次写入产生的交互和任务记录，聚焦后续行为
        clearInvocations(commandWriter, commandStoreDelay, offsetNotifier);
        writeSizes.clear();

        ByteBuf secondData = Unpooled.wrappedBuffer(new byte[100]); // 总大小 250 ≥ 200
        window.write(secondData);

        verify(commandStoreDelay).beginWrite();
        verify(commandWriter).write(any(ByteBuf.class));
        long offset = totalLength.get() - 1;
        verify(commandStoreDelay).endWrite(offset);
        verify(offsetNotifier).offsetIncreased(offset);
        assertEquals("刷盘大小应为窗口总大小", Integer.valueOf(250), writeSizes.get(0));
        // 首次写入调度的任务在刷盘时被取消
        verify(scheduledFutures.get(0), times(1)).cancel(false);

        firstData.release();
        secondData.release();
    }

    @Test
    public void testHighRateFlushOnMaxStayTime() throws IOException {
        when(keeperConfig.getCmdBatchFlushIntervalMillis()).thenReturn(100L);
        setRate(1_000_000.0);

        ByteBuf data = Unpooled.wrappedBuffer(new byte[100]);
        window.write(data);
        assertEquals("应生成一个定时任务", 1, scheduledTasks.size());
        verify(commandWriter, never()).write(any(ByteBuf.class));

        clearInvocations(commandWriter, commandStoreDelay, offsetNotifier);
        writeSizes.clear();
        // 手动触发定时任务，模拟超时
        scheduledTasks.get(0).run();

        verify(commandStoreDelay).beginWrite();
        verify(commandWriter).write(any(ByteBuf.class));
        long offset = totalLength.get() - 1;
        verify(commandStoreDelay).endWrite(offset);
        verify(offsetNotifier).offsetIncreased(offset);
        assertEquals("定时刷盘应写出窗口数据", Integer.valueOf(100), writeSizes.get(0));
        verify(scheduledFutures.get(0), times(1)).cancel(false);

        data.release();
    }

    @Test
    public void testRateDecayToLowThenFlushBufferAndSingle() throws IOException {
        setRate(1_000_000.0);
        ByteBuf firstData = Unpooled.wrappedBuffer(new byte[200]);
        window.write(firstData);

        // 速率降为零，模拟低速
        setRate(0.0);
        clearInvocations(commandWriter, commandStoreDelay, offsetNotifier);
        writeSizes.clear();

        ByteBuf secondData = Unpooled.wrappedBuffer(new byte[50]);
        window.write(secondData);

        // 预期：先刷窗口（200），再直接写新数据（50）
        verify(commandWriter, times(2)).write(any(ByteBuf.class));
        verify(commandStoreDelay, times(2)).beginWrite();
        assertEquals(2, writeSizes.size());
        assertEquals(Integer.valueOf(200), writeSizes.get(0));
        assertEquals(Integer.valueOf(50), writeSizes.get(1));
        verify(offsetNotifier, times(2)).offsetIncreased(anyLong());

        firstData.release();
        secondData.release();
    }

    @Test
    public void testFlushAll() throws IOException {
        setRate(1_000_000.0);
        ByteBuf data = Unpooled.wrappedBuffer(new byte[150]);
        window.write(data);

        clearInvocations(commandWriter, commandStoreDelay, offsetNotifier);
        writeSizes.clear();

        window.flushAll();

        verify(commandStoreDelay).beginWrite();
        verify(commandWriter).write(any(ByteBuf.class));
        assertEquals(Integer.valueOf(150), writeSizes.get(0));
        long offset = totalLength.get() - 1;
        verify(commandStoreDelay).endWrite(offset);
        verify(offsetNotifier).offsetIncreased(offset);
        verify(scheduledFutures.get(0), times(1)).cancel(false);

        data.release();
    }

    @Test
    public void testFlushAllEmptyWindow() throws IOException {
        clearInvocations(commandWriter);
        window.flushAll();
        verify(commandWriter, never()).write(any(ByteBuf.class));
    }

    @Test
    public void testClose() throws IOException {
        setRate(1_000_000.0);
        ByteBuf data = Unpooled.wrappedBuffer(new byte[300]);
        window.write(data);

        // 让 eventLoopGroup.execute 同步执行任务
        doAnswer(inv -> {
            ((Runnable) inv.getArgument(0)).run();
            return null;
        }).when(eventLoopGroup).execute(any(Runnable.class));

        clearInvocations(commandWriter, commandStoreDelay, offsetNotifier);
        writeSizes.clear();

        window.close();

        verify(commandStoreDelay).beginWrite();
        verify(commandWriter).write(any(ByteBuf.class));
        assertEquals(Integer.valueOf(300), writeSizes.get(0));
        long offset = totalLength.get() - 1;
        verify(commandStoreDelay).endWrite(offset);
        verify(offsetNotifier).offsetIncreased(offset);
        verify(scheduledFutures.get(0), times(1)).cancel(false);

        data.release();
    }
}