package com.ctrip.xpipe.redis.core.store.ck;

import com.lmax.disruptor.EventFactory;
import com.lmax.disruptor.YieldingWaitStrategy;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;
import com.lmax.disruptor.util.DaemonThreadFactory;

// 2. 实现事件工厂 - 这是预分配的核心！
public class MessageEventFactory implements EventFactory<MessageEvent> {
    @Override
    public MessageEvent newInstance() {
        return new MessageEvent();
    }

    public static void main(String[] args) {
        MessageEventFactory factory = new MessageEventFactory();
        int ringBufferSize = 1024 * 1024; // 1M个槽位

        Disruptor<MessageEvent> disruptor = new Disruptor<>(
                factory,                    // 事件工厂
                ringBufferSize,             // RingBuffer大小
                DaemonThreadFactory.INSTANCE,
                ProducerType.MULTI,
                new YieldingWaitStrategy()
        );
        disruptor.start();
    }
}
