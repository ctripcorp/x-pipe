package com.ctrip.xpipe.redis.keeper.monitor.impl;

import com.ctrip.xpipe.redis.core.store.ReplId;
import com.ctrip.xpipe.redis.keeper.RedisSlave;
import com.ctrip.xpipe.redis.keeper.storage.TransferToDelayContext;
import com.ctrip.xpipe.redis.keeper.storage.TransferToDelayHook;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLongArray;
import java.util.function.IntSupplier;

/**
 * Per-slave bounded transfer delay monitor. Slots are overwritten intentionally: monitoring must
 * never apply backpressure to replication. Offset matching drops updates from overwritten contexts.
 */
public class DefaultTransferToDelayHook implements TransferToDelayHook {

    private static final Logger logger = LoggerFactory.getLogger(DefaultTransferToDelayHook.class);

    public static final String KEY_TRANSFER_DELAY_SLOTS = "keeper.transfer.delay.monitor.slots";

    private static final int DEFAULT_TRANSFER_DELAY_SLOTS = 30;

    private static final int MAX_TRANSFER_DELAY_SLOTS = 1024;

    private static final long INVALID_OFFSET = Long.MIN_VALUE;

    private static final TransferToDelayContext.Stage[] STAGES = TransferToDelayContext.Stage.values();

    private static final int STAGE_COUNT = STAGES.length;

    private final ReplId replId;

    private final RedisSlave redisSlave;

    private final IntSupplier thresholdMicros;

    private final int slotCount;

    private final AtomicLongArray offsets;

    private final AtomicLongArray positions;

    private final AtomicLongArray thresholdNanos;

    private final AtomicLongArray stageStartedNanos;

    private final AtomicInteger cursor = new AtomicInteger();

    public DefaultTransferToDelayHook(ReplId replId, RedisSlave redisSlave, IntSupplier thresholdMicros) {
        this(replId, redisSlave, thresholdMicros, configuredSlotCount());
    }

    DefaultTransferToDelayHook(ReplId replId, RedisSlave redisSlave, IntSupplier thresholdMicros, int slotCount) {
        this.replId = replId;
        this.redisSlave = redisSlave;
        this.thresholdMicros = thresholdMicros;
        this.slotCount = Math.max(1, slotCount);
        this.offsets = new AtomicLongArray(this.slotCount);
        this.positions = new AtomicLongArray(this.slotCount);
        this.thresholdNanos = new AtomicLongArray(this.slotCount);
        this.stageStartedNanos = new AtomicLongArray(this.slotCount * STAGE_COUNT);
        for (int i = 0; i < this.slotCount; i++) {
            offsets.set(i, INVALID_OFFSET);
        }
    }

    private static int configuredSlotCount() {
        String configured = System.getProperty(KEY_TRANSFER_DELAY_SLOTS);
        if (configured == null) {
            return DEFAULT_TRANSFER_DELAY_SLOTS;
        }
        try {
            int slotCount = Integer.parseInt(configured);
            if (slotCount <= 0 || slotCount > MAX_TRANSFER_DELAY_SLOTS) {
                logger.warn("[transferToDelay][invalid slots] {}={}, use default {}",
                        KEY_TRANSFER_DELAY_SLOTS, configured, DEFAULT_TRANSFER_DELAY_SLOTS);
                return DEFAULT_TRANSFER_DELAY_SLOTS;
            }
            return slotCount;
        } catch (NumberFormatException e) {
            logger.warn("[transferToDelay][invalid slots] {}={}, use default {}",
                    KEY_TRANSFER_DELAY_SLOTS, configured, DEFAULT_TRANSFER_DELAY_SLOTS, e);
            return DEFAULT_TRANSFER_DELAY_SLOTS;
        }
    }

    @Override
    public TransferToDelayContext begin(long position, long offset) {
        int threshold = thresholdMicros.getAsInt();
        if (threshold <= 0 || offset == INVALID_OFFSET) {
            return TransferToDelayContext.NONE;
        }

        int slotIndex = Math.floorMod(cursor.getAndIncrement(), slotCount);
        reset(slotIndex, position, offset, threshold);
        return new Context(this, slotIndex, offset);
    }

    private void reset(int slotIndex, long position, long offset, long thresholdMicros) {
        offsets.set(slotIndex, INVALID_OFFSET);
        int stageBase = stageBase(slotIndex);
        for (int i = 0; i < STAGE_COUNT; i++) {
            stageStartedNanos.set(stageBase + i, 0L);
        }
        positions.set(slotIndex, position);
        thresholdNanos.set(slotIndex, TimeUnit.MICROSECONDS.toNanos(thresholdMicros));
        offsets.set(slotIndex, offset);
    }

    private void begin(int slotIndex, long expectedOffset, TransferToDelayContext.Stage stage) {
        if (!isCurrent(slotIndex, expectedOffset)) {
            return;
        }
        int stageIndex = stageIndex(slotIndex, stage);
        long started = System.nanoTime();
        if (stageStartedNanos.compareAndSet(stageIndex, 0L, started)
                && !isCurrent(slotIndex, expectedOffset)) {
            stageStartedNanos.compareAndSet(stageIndex, started, 0L);
        }
    }

    private void end(int slotIndex, long expectedOffset, TransferToDelayContext.Stage stage) {
        finishStage(slotIndex, expectedOffset, stage, System.nanoTime());
    }

    private void finishStage(int slotIndex, long expectedOffset,
            TransferToDelayContext.Stage stage, long now) {
        if (!isCurrent(slotIndex, expectedOffset)) {
            return;
        }
        int stageIndex = stageIndex(slotIndex, stage);
        long started = stageStartedNanos.get(stageIndex);
        if (started == 0L || !stageStartedNanos.compareAndSet(stageIndex, started, 0L)) {
            return;
        }
        if (!isCurrent(slotIndex, expectedOffset)) {
            return;
        }
        long durationNanos = Math.max(0L, now - started);
        long threshold = thresholdNanos.get(slotIndex);
        long position = positions.get(slotIndex);
        if (!isCurrent(slotIndex, expectedOffset)) {
            return;
        }
        logSlowStage(stage, position, expectedOffset, durationNanos, threshold);
    }

    private void complete(int slotIndex, long expectedOffset) {
        long now = System.nanoTime();
        for (TransferToDelayContext.Stage stage : STAGES) {
            finishStage(slotIndex, expectedOffset, stage, now);
        }
        offsets.compareAndSet(slotIndex, expectedOffset, INVALID_OFFSET);
    }

    private void logSlowStage(TransferToDelayContext.Stage stage, long position,
            long offset, long durationNanos, long threshold) {
        if (durationNanos < threshold) {
            return;
        }

        String slave = redisSlave.toString();

        logger.info("[transferToDelay][{}][{}] {}, delay:{}us position:{} offset:{}", replId, stage,
                slave, TimeUnit.NANOSECONDS.toMicros(durationNanos), position, offset);
    }

    private boolean isCurrent(int slotIndex, long expectedOffset) {
        return offsets.get(slotIndex) == expectedOffset;
    }

    private static int stageBase(int slotIndex) {
        return slotIndex * STAGE_COUNT;
    }

    private static int stageIndex(int slotIndex, TransferToDelayContext.Stage stage) {
        return stageBase(slotIndex) + stage.ordinal();
    }

    private static final class Context implements TransferToDelayContext {

        private final DefaultTransferToDelayHook owner;

        private final int slotIndex;

        private final long offset;

        private Context(DefaultTransferToDelayHook owner, int slotIndex, long offset) {
            this.owner = owner;
            this.slotIndex = slotIndex;
            this.offset = offset;
        }

        @Override
        public boolean isEnabled() {
            return true;
        }

        @Override
        public void begin(Stage stage) {
            owner.begin(slotIndex, offset, stage);
        }

        @Override
        public void end(Stage stage) {
            owner.end(slotIndex, offset, stage);
        }

        @Override
        public void complete(long transferredBytes, Throwable failure) {
            owner.complete(slotIndex, offset);
        }
    }
}
