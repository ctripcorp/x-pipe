package com.ctrip.xpipe.redis.keeper.health.rule;

import com.ctrip.xpipe.redis.keeper.storage.health.IoSource;

import java.util.Objects;

/** Structured evidence explaining why an IO health rule fired. */
public final class RuleTrigger {

    public enum Scope {
        CONTAINER,
        TENANT
    }

    public enum Signal {
        IN_FLIGHT_NANOS,
        MAX_EXEC_NANOS,
        FAILURE_RATIO,
        SLOW_RATIO,
        TENANT_ABNORMAL,
        FILE_CONSECUTIVE_FAILURES,
        FILE_FAILURES_IN_WINDOW
    }

    private final String rule;
    private final Scope scope;
    private final IoSource source;
    private final String tenant;
    private final String path;
    private final Signal signal;
    private final long actualValue;
    private final long thresholdValue;
    private final long sampleCount;

    private RuleTrigger(String rule, Scope scope, IoSource source, String tenant, String path,
            Signal signal, long actualValue, long thresholdValue, long sampleCount) {
        this.rule = Objects.requireNonNull(rule, "rule");
        this.scope = Objects.requireNonNull(scope, "scope");
        this.source = Objects.requireNonNull(source, "source");
        this.tenant = tenant;
        this.path = path;
        this.signal = Objects.requireNonNull(signal, "signal");
        this.actualValue = actualValue;
        this.thresholdValue = thresholdValue;
        this.sampleCount = sampleCount;
    }

    public static RuleTrigger container(IoHealthRule rule, IoSource source, String path,
            Signal signal, long actualValue, long thresholdValue, long sampleCount) {
        return new RuleTrigger(rule.name(), Scope.CONTAINER, source, null, path,
                signal, actualValue, thresholdValue, sampleCount);
    }

    public static RuleTrigger tenant(IoHealthRule rule, IoSource source, String tenant, String path,
            Signal signal, long actualValue, long thresholdValue, long sampleCount) {
        return new RuleTrigger(rule.name(), Scope.TENANT, source, tenant, path,
                signal, actualValue, thresholdValue, sampleCount);
    }

    public String rule() {
        return rule;
    }

    public Scope scope() {
        return scope;
    }

    public IoSource source() {
        return source;
    }

    public String tenant() {
        return tenant;
    }

    public String path() {
        return path;
    }

    public Signal signal() {
        return signal;
    }

    /**
     * Observed count or duration. Ratio signals use this as the numerator and
     * {@link #sampleCount()} as the denominator; their threshold is a percentage.
     */
    public long actualValue() {
        return actualValue;
    }

    /** Duration/count threshold, or percentage for ratio signals. */
    public long thresholdValue() {
        return thresholdValue;
    }

    /** Ratio denominator or relevant sample count; zero when not applicable. */
    public long sampleCount() {
        return sampleCount;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof RuleTrigger)) {
            return false;
        }
        RuleTrigger that = (RuleTrigger) o;
        return actualValue == that.actualValue
                && thresholdValue == that.thresholdValue
                && sampleCount == that.sampleCount
                && rule.equals(that.rule)
                && scope == that.scope
                && source == that.source
                && Objects.equals(tenant, that.tenant)
                && Objects.equals(path, that.path)
                && signal == that.signal;
    }

    @Override
    public int hashCode() {
        return Objects.hash(rule, scope, source, tenant, path, signal,
                actualValue, thresholdValue, sampleCount);
    }

    @Override
    public String toString() {
        return "RuleTrigger{" +
                "rule='" + rule + '\'' +
                ", scope=" + scope +
                ", source=" + source +
                ", tenant='" + tenant + '\'' +
                ", path='" + path + '\'' +
                ", signal=" + signal +
                ", actualValue=" + actualValue +
                ", thresholdValue=" + thresholdValue +
                ", sampleCount=" + sampleCount +
                '}';
    }
}
