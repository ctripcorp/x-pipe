package com.ctrip.xpipe.redis.comparator.compare;

import com.ctrip.xpipe.redis.comparator.stream.StreamRingBuffer;

/**
 * 比对器看到的一路流（spec D2 / D33 ④ / D35 ③）。单路流不自行判定 replId。
 * <p>
 * RingBuffer 与流 generation 绑定：{@link #disconnect()} 废弃当前写侧，
 * {@link #getReplId()} 立刻为 {@code null}；{@link #reconnect()} 起新的 {@code ? -4}，
 * 写侧在 {@code onKeeperContinue} 按新 offset 换新 {@link StreamRingBuffer}。
 * 比对线程禁止对正在被 IO 写的 buffer 原地 clear。
 */
public interface CompareLane {

    /**
     * 单路流 generation 的一致快照。生产流应在其 session 锁内覆盖此方法；
     * 默认实现保持测试 lane 与简单实现的单次 replId 读取语义。
     */
    default Generation generation() {
        String replId = getReplId();
        if (replId == null) {
            return null;
        }
        return new Generation(replId, getContinueOffset(), getBuffer());
    }

    String getAddress();

    /**
     * CONTINUE 的 replId。尚未 CONTINUE、已 {@link #disconnect()}、
     * 或刚 {@link #reconnect()} 后为 {@code null}。
     */
    String getReplId();

    /**
     * CONTINUE 给出的第一个待发字节（不 {@code +1}）。
     * {@link #getReplId()} 为 {@code null} 时不得使用。
     */
    long getContinueOffset();

    /**
     * 当前 generation 的 buffer。{@code disconnect} 之后、新 CONTINUE 之前
     * 可能仍是旧实例或占位；{@code alignStart} 必须以 {@link #getReplId()}
     * 为 {@code null} 作为门禁，不得 peek。
     */
    StreamRingBuffer getBuffer();

    /**
     * 停本路写侧。返回后 {@link #getReplId()} 必须为 {@code null}。
     * 旧连接 in-flight {@code onCommands} 只允许写入已废弃的 buffer 实例。
     */
    void disconnect();

    /**
     * 停写当前 session（与是否先 {@link #disconnect()} 无关），再起新的 {@code ? -4}。
     * CONTINUE 时由写侧换新 RingBuffer（{@code streamStart} 即 continue offset；
     * {@code streamStart} 不可变，不能原地改起点）。
     */
    void reconnect();

    /**
     * 释放本路（停自动建流并拆连接）。幂等。
     * 由打开方 {@code ShardCompareTaskManager} 调用，比对器 {@code stop} 不 close。
     */
    void close();

    /**
     * 本路重连次数。默认 0；生产 {@code KeeperReplStream} 覆盖。
     * 给 {@code /api/status} 用，不进 Hickwall（D36 ② 只打 {@code comparedBytes}）。
     */
    default int getStreamReconnectCount() {
        return 0;
    }

    final class Generation {
        private final String replId;
        private final long continueOffset;
        private final StreamRingBuffer buffer;

        public Generation(String replId, long continueOffset, StreamRingBuffer buffer) {
            this.replId = replId;
            this.continueOffset = continueOffset;
            this.buffer = buffer;
        }

        public String getReplId() {
            return replId;
        }

        public long getContinueOffset() {
            return continueOffset;
        }

        public StreamRingBuffer getBuffer() {
            return buffer;
        }
    }
}
