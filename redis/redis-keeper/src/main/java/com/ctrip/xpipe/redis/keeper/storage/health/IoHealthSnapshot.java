package com.ctrip.xpipe.redis.keeper.storage.health;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/** IO statistics of one tick. Thresholds live in the rule layer. */
public final class IoHealthSnapshot {

    private final long sampleNanos;
    private final SourceSnapshot business;
    private final ProbeSnapshot probe;
    private final Map<String, TenantStat> tenants;

    IoHealthSnapshot(long sampleNanos, SourceSnapshot business, ProbeSnapshot probe,
            Map<String, TenantStat> tenants) {
        this.sampleNanos = sampleNanos;
        this.business = business;
        this.probe = probe;
        this.tenants = Collections.unmodifiableMap(tenants);
    }

    public long sampleNanos() {
        return sampleNanos;
    }

    /** Business IO snapshot. Legacy top-level getters below delegate to this object. */
    public SourceSnapshot business() {
        return business;
    }

    /** Probe task and logical probe-round snapshot. */
    public ProbeSnapshot probe() {
        return probe;
    }

    public long total() { return business.total(); }
    public long failed() { return business.failed(); }
    public long slow() { return business.slow(); }
    public long inFlight() { return business.inFlight(); }
    public long slowInFlight() { return business.slowInFlight(); }
    public long maxInFlightNanos() { return business.maxInFlightNanos(); }
    public long maxEndToEndNanos() { return business.maxEndToEndNanos(); }
    public long maxExecNanos() { return business.maxExecNanos(); }

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
        private long probeTotal;
        private long probeFailed;
        private long probeSlow;
        private long probeInFlight;
        private long probeSlowInFlight;
        private long probeMaxInFlightNanos;
        private long probeMaxEndToEndNanos;
        private long probeMaxExecNanos;
        private long probeRounds;
        private long probeSlowRounds;
        private final Map<String, TenantStat> tenants = new HashMap<>();
        private final Map<String, FileStat> probeFiles = new HashMap<>();

        public Builder sampleNanos(long v) { this.sampleNanos = v; return this; }
        public Builder total(long v) { this.total = v; return this; }
        public Builder failed(long v) { this.failed = v; return this; }
        public Builder slow(long v) { this.slow = v; return this; }
        public Builder inFlight(long v) { this.inFlight = v; return this; }
        public Builder slowInFlight(long v) { this.slowInFlight = v; return this; }
        public Builder maxInFlightNanos(long v) { this.maxInFlightNanos = v; return this; }
        public Builder maxEndToEndNanos(long v) { this.maxEndToEndNanos = v; return this; }
        public Builder maxExecNanos(long v) { this.maxExecNanos = v; return this; }

        public Builder probe(long total, long failed, long slow, long inFlight,
                long slowInFlight, long maxInFlightNanos, long maxEndToEndNanos,
                long maxExecNanos, long rounds, long slowRounds) {
            this.probeTotal = total;
            this.probeFailed = failed;
            this.probeSlow = slow;
            this.probeInFlight = inFlight;
            this.probeSlowInFlight = slowInFlight;
            this.probeMaxInFlightNanos = maxInFlightNanos;
            this.probeMaxEndToEndNanos = maxEndToEndNanos;
            this.probeMaxExecNanos = maxExecNanos;
            this.probeRounds = rounds;
            this.probeSlowRounds = slowRounds;
            return this;
        }

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

        public Builder probeFile(String path, long consecutiveFails, long fails10, long lastEndNanos) {
            this.probeFiles.put(path, FileStat.of(null, path, consecutiveFails, fails10, lastEndNanos));
            return this;
        }

        public IoHealthSnapshot build() {
            SourceSnapshot business = new SourceSnapshot(sampleNanos, total, failed, slow,
                    inFlight, slowInFlight, maxInFlightNanos, maxEndToEndNanos, maxExecNanos);
            ProbeSnapshot probe = new ProbeSnapshot(
                    new SourceSnapshot(sampleNanos, probeTotal, probeFailed, probeSlow,
                            probeInFlight, probeSlowInFlight, probeMaxInFlightNanos,
                            probeMaxEndToEndNanos, probeMaxExecNanos),
                    probeRounds, probeSlowRounds, probeFiles);
            return new IoHealthSnapshot(sampleNanos, business, probe, tenants);
        }
    }

    public static class SourceSnapshot {
        private final long sampleNanos;
        private final long total;
        private final long failed;
        private final long slow;
        private final long inFlight;
        private final long slowInFlight;
        private final long maxInFlightNanos;
        private final long maxEndToEndNanos;
        private final long maxExecNanos;

        SourceSnapshot(long sampleNanos, long total, long failed, long slow, long inFlight,
                long slowInFlight, long maxInFlightNanos, long maxEndToEndNanos, long maxExecNanos) {
            this.sampleNanos = sampleNanos;
            this.total = total;
            this.failed = failed;
            this.slow = slow;
            this.inFlight = inFlight;
            this.slowInFlight = slowInFlight;
            this.maxInFlightNanos = maxInFlightNanos;
            this.maxEndToEndNanos = maxEndToEndNanos;
            this.maxExecNanos = maxExecNanos;
        }

        public long sampleNanos() { return sampleNanos; }
        public long total() { return total; }
        public long failed() { return failed; }
        public long slow() { return slow; }
        public long inFlight() { return inFlight; }
        public long slowInFlight() { return slowInFlight; }
        public long maxInFlightNanos() { return maxInFlightNanos; }
        public long maxEndToEndNanos() { return maxEndToEndNanos; }
        public long maxExecNanos() { return maxExecNanos; }
    }

    public static final class ProbeSnapshot {
        private final SourceSnapshot tasks;
        private final long rounds;
        private final long slowRounds;
        private final Map<String, FileStat> files;

        ProbeSnapshot(SourceSnapshot tasks, long rounds, long slowRounds,
                Map<String, FileStat> files) {
            this.tasks = tasks;
            this.rounds = rounds;
            this.slowRounds = slowRounds;
            this.files = Collections.unmodifiableMap(new HashMap<>(files));
        }

        public SourceSnapshot tasks() { return tasks; }
        public long rounds() { return rounds; }
        public long slowRounds() { return slowRounds; }
        public Map<String, FileStat> files() { return files; }
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

        public Map<String, FileStat> files() { return files; }
        public long total() { return total; }
        public long abnormal() { return abnormal; }
        public long inFlight() { return inFlight; }
        public long slowInFlight() { return slowInFlight; }
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

        public String tenant() { return tenant; }
        public String path() { return path; }
        public long fails10() { return fails10; }
        public long consecutiveFails() { return consecutiveFails; }
        public long lastEndNanos() { return lastEndNanos; }
    }
}
