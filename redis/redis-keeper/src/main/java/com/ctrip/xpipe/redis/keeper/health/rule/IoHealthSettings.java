package com.ctrip.xpipe.redis.keeper.health.rule;

import com.ctrip.xpipe.redis.keeper.config.KeeperContainerConfig;
import com.ctrip.xpipe.redis.keeper.storage.health.IoTelemetry;
import com.ctrip.xpipe.redis.keeper.storage.health.IoTelemetrySettings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

/** Every tunable of the IO health feature: the measurement knobs plus the rule thresholds. */
public final class IoHealthSettings implements IoTelemetrySettings {

    private static final Logger logger = LoggerFactory.getLogger(IoHealthSettings.class);

    private final boolean enabled;
    private final long slowNanos;
    private final long windowNanos;
    private final long fileWindowNanos;
    private final long stateIdleNanos;
    private final long probeWindowNanos;
    private final RuleConfig rules;

    private IoHealthSettings(boolean enabled, long slowNanos, long windowNanos, long fileWindowNanos,
            long stateIdleNanos, long probeWindowNanos, RuleConfig rules) {
        this.enabled = enabled;
        this.slowNanos = slowNanos;
        this.windowNanos = windowNanos;
        this.fileWindowNanos = fileWindowNanos;
        this.stateIdleNanos = stateIdleNanos;
        this.probeWindowNanos = probeWindowNanos;
        this.rules = rules;
    }

    public static IoHealthSettings from(KeeperContainerConfig config) {
        return builder()
                .enabled(config.isTfsMode())
                .slowMillis(config.ioHealthSlowMillis())
                .windowSeconds(config.ioHealthWindowSeconds())
                .fileWindowSeconds(config.ioHealthFileWindowSeconds())
                .stateIdleSeconds(config.ioHealthStateIdleSeconds())
                .probeWindowSeconds(config.ioHealthProbeWindowSeconds())
                .rules(RuleConfig.from(config))
                .build();
    }

    public static IoHealthSettings defaults() {
        return builder().build();
    }

    @Override
    public boolean enabled() {
        return enabled;
    }

    @Override
    public long slowNanos() {
        return slowNanos;
    }

    @Override
    public long windowNanos() {
        return windowNanos;
    }

    @Override
    public long fileWindowNanos() {
        return fileWindowNanos;
    }

    @Override
    public long stateIdleNanos() {
        return stateIdleNanos;
    }

    @Override
    public long probeWindowNanos() {
        return probeWindowNanos;
    }

    public RuleConfig rules() {
        return rules;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static final class Builder {
        private boolean enabled = true;
        private long slowMillis = 2_000L;
        private long windowSeconds = 30L;
        private long fileWindowSeconds = 10L;
        private long stateIdleSeconds = 60L;
        private long probeWindowSeconds = 15L;
        private RuleConfig rules = RuleConfig.builder().build();

        public Builder enabled(boolean v) { this.enabled = v; return this; }
        public Builder slowMillis(long v) { this.slowMillis = v; return this; }
        public Builder windowSeconds(long v) { this.windowSeconds = v; return this; }
        public Builder fileWindowSeconds(long v) { this.fileWindowSeconds = v; return this; }
        public Builder stateIdleSeconds(long v) { this.stateIdleSeconds = v; return this; }
        public Builder probeWindowSeconds(long v) { this.probeWindowSeconds = v; return this; }
        public Builder rules(RuleConfig v) { this.rules = v == null ? RuleConfig.builder().build() : v; return this; }

        public IoHealthSettings build() {
            long window = TimeUnit.SECONDS.toNanos(Math.max(1L, windowSeconds));
            if (window > IoTelemetry.MAX_WINDOW_NANOS) {
                // the ring has one bucket per second; a wider window cannot be served
                logger.warn("[build]window {}s exceeds {}s, clamped", windowSeconds,
                        TimeUnit.NANOSECONDS.toSeconds(IoTelemetry.MAX_WINDOW_NANOS));
                window = IoTelemetry.MAX_WINDOW_NANOS;
            }
            return new IoHealthSettings(
                    enabled,
                    TimeUnit.MILLISECONDS.toNanos(Math.max(1L, slowMillis)),
                    window,
                    TimeUnit.SECONDS.toNanos(Math.max(1L, fileWindowSeconds)),
                    TimeUnit.SECONDS.toNanos(Math.max(1L, stateIdleSeconds)),
                    TimeUnit.SECONDS.toNanos(Math.max(1L, probeWindowSeconds)),
                    rules);
        }
    }
}
