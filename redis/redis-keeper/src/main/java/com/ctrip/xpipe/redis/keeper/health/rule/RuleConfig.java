package com.ctrip.xpipe.redis.keeper.health.rule;

import com.ctrip.xpipe.redis.keeper.config.KeeperContainerConfig;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;

public final class RuleConfig {

    public static final long DEFAULT_HANG_MILL = 5_000L;
    public static final long DEFAULT_MIN_WINDOW_RECORDS = 1_000L;

    private final long hangNanos;
    private final int failRatioPercent;
    private final int slowRatioPercent;
    private final long minWindowRecords;
    private final int tenantAbnormalPercent;
    private final int tenantAbnormalFloor;
    private final int fileConsecutiveFails;
    private final int fileFailsInWindow;
    private final Set<String> disabledRules;

    private RuleConfig(long hangNanos, int failRatioPercent, int slowRatioPercent, long minWindowRecords,
            int tenantAbnormalPercent, int tenantAbnormalFloor, int fileConsecutiveFails,
            int fileFailsInWindow, Set<String> disabledRules) {
        this.hangNanos = hangNanos;
        this.failRatioPercent = failRatioPercent;
        this.slowRatioPercent = slowRatioPercent;
        this.minWindowRecords = minWindowRecords;
        this.tenantAbnormalPercent = tenantAbnormalPercent;
        this.tenantAbnormalFloor = tenantAbnormalFloor;
        this.fileConsecutiveFails = fileConsecutiveFails;
        this.fileFailsInWindow = fileFailsInWindow;
        this.disabledRules = disabledRules;
    }

    public static RuleConfig from(KeeperContainerConfig config) {
        return builder()
                .hangMillis(config.ioHealthHangMillis())
                .failRatioPercent(config.ioHealthFailRatioPercent())
                .slowRatioPercent(config.ioHealthSlowRatioPercent())
                .minWindowRecords(config.ioHealthMinWindowRecords())
                .tenantAbnormalPercent(config.ioHealthTenantAbnormalPercent())
                .tenantAbnormalFloor(config.ioHealthTenantAbnormalFloor())
                .fileConsecutiveFails(config.ioHealthFileConsecutiveFails())
                .fileFailsInWindow(config.ioHealthFileFailsInWindow())
                .disabledRules(config.ioHealthDisabledRules())
                .build();
    }

    public long hangNanos() {
        return hangNanos;
    }

    public int failRatioPercent() {
        return failRatioPercent;
    }

    public int slowRatioPercent() {
        return slowRatioPercent;
    }

    public long minWindowRecords() {
        return minWindowRecords;
    }

    public int tenantAbnormalPercent() {
        return tenantAbnormalPercent;
    }

    public int tenantAbnormalFloor() {
        return tenantAbnormalFloor;
    }

    public int fileConsecutiveFails() {
        return fileConsecutiveFails;
    }

    public int fileFailsInWindow() {
        return fileFailsInWindow;
    }

    public boolean isEnabled(String ruleName) {
        return !disabledRules.contains(ruleName);
    }

    public static Builder builder() {
        return new Builder();
    }

    public static final class Builder {
        private long hangMillis = DEFAULT_HANG_MILL;
        private int failRatioPercent = 1;
        private int slowRatioPercent = 5;
        private long minWindowRecords = DEFAULT_MIN_WINDOW_RECORDS;
        private int tenantAbnormalPercent = 5;
        private int tenantAbnormalFloor = 5;
        private int fileConsecutiveFails = 3;
        private int fileFailsInWindow = 5;
        private Set<String> disabledRules = Collections.emptySet();

        public Builder hangMillis(long v) { this.hangMillis = v; return this; }
        public Builder failRatioPercent(int v) { this.failRatioPercent = v; return this; }
        public Builder slowRatioPercent(int v) { this.slowRatioPercent = v; return this; }
        public Builder minWindowRecords(long v) { this.minWindowRecords = v; return this; }
        public Builder tenantAbnormalPercent(int v) { this.tenantAbnormalPercent = v; return this; }
        public Builder tenantAbnormalFloor(int v) { this.tenantAbnormalFloor = v; return this; }
        public Builder fileConsecutiveFails(int v) { this.fileConsecutiveFails = v; return this; }
        public Builder fileFailsInWindow(int v) { this.fileFailsInWindow = v; return this; }

        public Builder disabledRules(String csv) {
            if (csv == null || csv.trim().isEmpty()) {
                this.disabledRules = Collections.emptySet();
                return this;
            }
            Set<String> rules = new HashSet<>();
            Arrays.stream(csv.split(",")).map(String::trim).filter(s -> !s.isEmpty()).forEach(rules::add);
            this.disabledRules = Collections.unmodifiableSet(rules);
            return this;
        }

        public RuleConfig build() {
            // clamped so a mocked or missing config cannot produce a rule that degrades everybody
            return new RuleConfig(
                    TimeUnit.MILLISECONDS.toNanos(Math.max(1L, hangMillis)),
                    Math.max(0, failRatioPercent),
                    Math.max(0, slowRatioPercent),
                    Math.max(0L, minWindowRecords),
                    Math.max(0, tenantAbnormalPercent),
                    Math.max(0, tenantAbnormalFloor),
                    Math.max(1, fileConsecutiveFails),
                    Math.max(1, fileFailsInWindow),
                    disabledRules);
        }
    }
}
