package com.ctrip.xpipe.redis.keeper.store.gtid.index;

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.*;

import com.ctrip.xpipe.redis.core.store.CommandWriter;
import com.ctrip.xpipe.redis.keeper.config.KeeperConfig;
import com.ctrip.xpipe.redis.keeper.monitor.CommandStoreDelay;
import com.ctrip.xpipe.utils.OffsetNotifier;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.util.concurrent.ScheduledFuture;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

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
    // 记录每次 write(ByteBuf) 的实际字节数
    private final List<Integer> writeSizes = new ArrayList<>();
    // 记录提交到 eventLoopGroup 的定时任务及对应的 ScheduledFuture
    private final List<Runnable> scheduledTasks = new ArrayList<>();
    private final List<ScheduledFuture<?>> scheduledFutures = new ArrayList<>();

    @Before
    public void setUp() throws Exception {
        // 默认阈值 1024，最大驻留时间 200ms
        when(keeperConfig.getCmdBatchWriteSize()).thenReturn(1024);
        when(keeperConfig.getCmdBatchFlushIntervalMillis()).thenReturn(200L);

        writeSizes.clear();
        // 模拟 commandWriter.write()：累加总长度并记录写入量
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
        // 模拟 eventLoopGroup.schedule：捕获任务，返回可控的 ScheduledFuture
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

    // ==================== 核心场景测试 ====================


    /**
     * 写入小于阈值，数据留在窗口并启动延迟刷新定时器
     */
    @Test
    public void testWriteBelowThresholdStartsTimer() throws IOException {
        ByteBuf data = Unpooled.wrappedBuffer(new byte[100]);

        window.write(data);

        // 不应立即刷盘
        verify(commandWriter, never()).write(any(ByteBuf.class));
        verify(commandStoreDelay, never()).beginWrite();
        // 必须启动了一个定时任务
        assertEquals(1, scheduledTasks.size());
    }

    /**
     * 多次写入累积达到阈值时，将所有数据一起刷盘，并取消之前的定时器
     */
    @Test
    public void testMultipleWritesFlushOnThreshold() throws IOException {
        // 动态修改阈值为 200
        when(keeperConfig.getCmdBatchWriteSize()).thenReturn(200);

        // 第一次写入 120，不触发刷盘
        ByteBuf data1 = Unpooled.wrappedBuffer(new byte[120]);
        window.write(data1);
        assertEquals(1, scheduledTasks.size());
        clearInvocations(commandWriter, commandStoreDelay, offsetNotifier);
        writeSizes.clear();

        // 第二次写入 100，总大小 220 >= 200，触发刷盘
        ByteBuf data2 = Unpooled.wrappedBuffer(new byte[100]);
        window.write(data2);

        // 应刷盘一次，包含所有累积数据 (120+100=220)
        verify(commandStoreDelay).beginWrite();
        verify(commandWriter).write(any(ByteBuf.class));
        long offset = totalLength.get() - 1;
        verify(commandStoreDelay).endWrite(offset);
        verify(offsetNotifier).offsetIncreased(offset);
        assertEquals(Collections.singletonList(220), writeSizes);
        // 之前的定时器应被取消
        verify(scheduledFutures.get(0)).cancel(false);
        // 刷盘后窗口为空，不应再有新定时器（除非又有新写入）
        assertEquals(1, scheduledFutures.size()); // 之前只 schedule 了一次
    }

    /**
     * 数据留在窗口中，定时器到期后自动刷盘
     */
    @Test
    public void testTimerFiresAndFlush() throws IOException {
        // 写入不触发刷盘的数据
        ByteBuf data = Unpooled.wrappedBuffer(new byte[80]);
        window.write(data);
        assertEquals(1, scheduledTasks.size());

        // 模拟定时器触发
        scheduledTasks.get(0).run();

        // 验证刷盘行为
        verify(commandStoreDelay).beginWrite();
        verify(commandWriter).write(any(ByteBuf.class));
        long offset = totalLength.get() - 1;
        verify(commandStoreDelay).endWrite(offset);
        verify(offsetNotifier).offsetIncreased(offset);
        assertEquals(Collections.singletonList(80), writeSizes);
    }

    /**
     * flushAll 将窗口内剩余数据刷盘
     */
    @Test
    public void testFlushAllFlushesRemainingData() throws IOException {
        // 写入一批数据，留在窗口
        ByteBuf data = Unpooled.wrappedBuffer(new byte[300]);
        window.write(data);
        // 清除之前可能的 write 调用（本例中不应有）
        clearInvocations(commandWriter, commandStoreDelay, offsetNotifier);
        writeSizes.clear();

        window.flushAll();

        verify(commandStoreDelay).beginWrite();
        verify(commandWriter).write(any(ByteBuf.class));
        assertEquals(Collections.singletonList(300), writeSizes);
        long offset = totalLength.get() - 1;
        verify(commandStoreDelay).endWrite(offset);
        verify(offsetNotifier).offsetIncreased(offset);
    }

    /**
     * 空窗口调用 flushAll 不应有任何操作
     */
    @Test
    public void testFlushAllEmptyWindowNoOp() throws IOException {
        window.flushAll();
        verify(commandWriter, never()).write(any(ByteBuf.class));
    }

    /**
     * close 会刷出所有剩余数据
     */
    @Test
    public void testCloseFlushesRemainingData() throws IOException {
        ByteBuf data = Unpooled.wrappedBuffer(new byte[500]);
        window.write(data);
        clearInvocations(commandWriter, commandStoreDelay, offsetNotifier);
        writeSizes.clear();

        window.close();

        verify(commandStoreDelay).beginWrite();
        verify(commandWriter).write(any(ByteBuf.class));
        assertEquals(Collections.singletonList(500), writeSizes);
        long offset = totalLength.get() - 1;
        verify(commandStoreDelay).endWrite(offset);
        verify(offsetNotifier).offsetIncreased(offset);
    }


    /**
     * 动态修改阈值，后续写入使用新阈值判断
     */
    @Test
    public void testDynamicThresholdChange() throws IOException {
        // 初始阈值 1024，写入 500 不刷盘
        ByteBuf data1 = Unpooled.wrappedBuffer(new byte[500]);
        window.write(data1);
        assertEquals(1, scheduledTasks.size());
        verify(commandWriter, never()).write(any(ByteBuf.class));

        // 动态调低阈值到 600，再写入 200，总大小 700 >= 600，触发刷盘
        when(keeperConfig.getCmdBatchWriteSize()).thenReturn(600);
        ByteBuf data2 = Unpooled.wrappedBuffer(new byte[200]);
        clearInvocations(commandWriter, commandStoreDelay, offsetNotifier);
        writeSizes.clear();
        window.write(data2);

        verify(commandStoreDelay).beginWrite();
        verify(commandWriter).write(any(ByteBuf.class));
        assertEquals(Collections.singletonList(700), writeSizes);
        // 旧的定时器应被取消
        verify(scheduledFutures.get(0)).cancel(false);
    }
}