package com.ctrip.xpipe.redis.keeper.storage.health;

import java.util.concurrent.TimeUnit;

/** Measurement knobs of {@link IoTelemetry}; the rule layer's IoHealthSettings also implements this. */
public interface IoTelemetrySettings {

    /** False makes {@link IoTelemetry} drop every task without recording (non-TFS containers). */
    default boolean enabled() {
        return true;
    }

    long slowNanos();

    long windowNanos();

    long fileWindowNanos();

    long stateIdleNanos();

    default long probeWindowNanos() {
        return windowNanos();
    }

    IoTelemetrySettings DEFAULTS = new IoTelemetrySettings() {

        @Override
        public long slowNanos() {
            return TimeUnit.SECONDS.toNanos(2);
        }

        @Override
        public long windowNanos() {
            return TimeUnit.SECONDS.toNanos(30);
        }

        @Override
        public long fileWindowNanos() {
            return TimeUnit.SECONDS.toNanos(10);
        }

        @Override
        public long stateIdleNanos() {
            return TimeUnit.MINUTES.toNanos(1);
        }
    };
}
