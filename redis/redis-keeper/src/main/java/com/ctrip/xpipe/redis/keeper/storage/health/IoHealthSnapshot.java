package com.ctrip.xpipe.redis.keeper.storage.health;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/** IO statistics of one tick. Thresholds live in the rule layer. */
public final class IoHealthSnapshot {

    private final long sampleNanos;
    private final long total;
    private final long failed;
    private final long slow;
    private final long inFlight;
    private final long slowInFlight;
    private final long maxInFlightNanos;
    private final long maxEndToEndNanos;
    private final long maxExecNanos;
    private final Map<String, TenantStat> tenants;

    IoHealthSnapshot(long sampleNanos, long total, long failed, long slow, long inFlight,
            long slowInFlight, long maxInFlightNanos, long maxEndToEndNanos, long maxExecNanos,
            Map<String, TenantStat> tenants) {
        this.sampleNanos = sampleNanos;
        this.total = total;
        this.failed = failed;
        this.slow = slow;
        this.inFlight = inFlight;
        this.slowInFlight = slowInFlight;
        this.maxInFlightNanos = maxInFlightNanos;
        this.maxEndToEndNanos = maxEndToEndNanos;
        this.maxExecNanos = maxExecNanos;
        this.tenants = Collections.unmodifiableMap(tenants);
    }

    public long sampleNanos() {
        return sampleNanos;
    }

    /** Every pool task (tenant==null included) inside the fixed 30s rule window. */
    public long total() {
        return total;
    }

    public long failed() {
        return failed;
    }

    /** Finished tasks whose execution time (queue wait excluded) is above the slow threshold. */
    public long slow() {
        return slow;
    }

    /** Tasks inside {@code run()} at the sample time; not part of {@link #total()}. */
    public long inFlight() {
        return inFlight;
    }

    /** Of {@link #inFlight()}, those already running longer than the slow threshold. */
    public long slowInFlight() {
        return slowInFlight;
    }

    /** Longest time a still-running task has been inside {@code run()}, queue wait excluded. */
    public long maxInFlightNanos() {
        return maxInFlightNanos;
    }

    /** Longest submit-to-end time among finished tasks in the window, queue wait included. */
    public long maxEndToEndNanos() {
        return maxEndToEndNanos;
    }

    public long maxExecNanos() {
        return maxExecNanos;
    }

    public Map<String, TenantStat> tenants() {
        return tenants;
    }

    public TenantStat tenant(String tenant) {
        return tenants.get(tenant);
    }

    /** Builds a snapshot for tests and synthetic rule inputs. */
    public static Builder builder() {
        return new Builder();
    }

    public static final class Builder {
        private long sampleNanos;
        private long total;
        private long failed;
        private long slow;
        private long inFlight;
        private long slowInFlight;
        private long maxInFlightNanos;
        private long maxEndToEndNanos;
        private long maxExecNanos;
        private final Map<String, TenantStat> tenants = new HashMap<>();

        public Builder sampleNanos(long v) { this.sampleNanos = v; return this; }
        public Builder total(long v) { this.total = v; return this; }
        public Builder failed(long v) { this.failed = v; return this; }
        public Builder slow(long v) { this.slow = v; return this; }
        public Builder inFlight(long v) { this.inFlight = v; return this; }
        public Builder slowInFlight(long v) { this.slowInFlight = v; return this; }
        public Builder maxInFlightNanos(long v) { this.maxInFlightNanos = v; return this; }
        public Builder maxEndToEndNanos(long v) { this.maxEndToEndNanos = v; return this; }
        public Builder maxExecNanos(long v) { this.maxExecNanos = v; return this; }

        public Builder tenant(String tenant, long total, long abnormal, long inFlight, long slowInFlight) {
            this.tenants.put(tenant, TenantStat.of(total, abnormal, inFlight, slowInFlight));
            return this;
        }

        /** Adds a file under its tenant, creating the tenant entry when needed. */
        public Builder file(String tenant, String path, long consecutiveFails, long fails10, long lastEndNanos) {
            TenantStat stat = this.tenants.computeIfAbsent(tenant, t -> TenantStat.of(0, 0, 0, 0));
            stat.files.put(path, FileStat.of(tenant, path, consecutiveFails, fails10, lastEndNanos));
            return this;
        }

        public IoHealthSnapshot build() {
            return new IoHealthSnapshot(sampleNanos, total, failed, slow, inFlight, slowInFlight,
                    maxInFlightNanos, maxEndToEndNanos, maxExecNanos, tenants);
        }
    }

    public static final class TenantStat {
        long total;
        long abnormal;
        long inFlight;
        long slowInFlight;
        final Map<String, FileStat> files = new HashMap<>();

        static TenantStat of(long total, long abnormal, long inFlight, long slowInFlight) {
            TenantStat stat = new TenantStat();
            stat.total = total;
            stat.abnormal = abnormal;
            stat.inFlight = inFlight;
            stat.slowInFlight = slowInFlight;
            return stat;
        }

        /** Files of this tenant seen inside the file rule window. */
        public Map<String, FileStat> files() {
            return files;
        }

        /** This tenant's finished tasks inside the fixed 30s rule window. */
        public long total() {
            return total;
        }

        /** Of {@link #total()}, those failed or slower than 2s. */
        public long abnormal() {
            return abnormal;
        }

        /** This tenant's tasks inside {@code run()} at the sample time; not part of {@link #total()}. */
        public long inFlight() {
            return inFlight;
        }

        /** Of {@link #inFlight()}, those already running longer than 2s. */
        public long slowInFlight() {
            return slowInFlight;
        }
    }

    public static final class FileStat {
        String tenant;
        String path;
        long fails10;
        long consecutiveFails;
        long lastEndNanos;

        static FileStat of(String tenant, String path, long consecutiveFails, long fails10, long lastEndNanos) {
            FileStat stat = new FileStat();
            stat.tenant = tenant;
            stat.path = path;
            stat.consecutiveFails = consecutiveFails;
            stat.fails10 = fails10;
            stat.lastEndNanos = lastEndNanos;
            return stat;
        }

        /** Owning keeper, so a rule does not have to parse the composite key. */
        public String tenant() {
            return tenant;
        }

        public String path() {
            return path;
        }

        public long fails10() {
            return fails10;
        }

        /** Trailing failures in the window; reset by any success of the same file. */
        public long consecutiveFails() {
            return consecutiveFails;
        }

        public long lastEndNanos() {
            return lastEndNanos;
        }
    }
}
