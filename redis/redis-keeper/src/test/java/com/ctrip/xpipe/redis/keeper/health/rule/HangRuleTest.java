package com.ctrip.xpipe.redis.keeper.health.rule;

import com.ctrip.xpipe.redis.keeper.storage.health.IoHealthSnapshot;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.TimeUnit;

public class HangRuleTest {

    private static final long HANG = RuleConfig.DEFAULT_HANG_MILL;

    private final HangRule rule = new HangRule();

    @Test
    public void testRunningTaskOverThresholdIsAHang() {
        RuleResult result = evaluate(IoHealthSnapshot.builder()
                .maxInFlightNanos(TimeUnit.MILLISECONDS.toNanos(HANG) + 1).build());
        Assert.assertTrue(result.isContainerAnomalous());
        Assert.assertTrue(result.triggered().contains(HangRule.NAME));
    }

    @Test
    public void testFinishedTaskOverThresholdIsAHang() {
        RuleResult result = evaluate(IoHealthSnapshot.builder()
                .maxExecNanos(TimeUnit.MILLISECONDS.toNanos(HANG) + 1).build());
        Assert.assertTrue(result.isContainerAnomalous());
    }

    @Test
    public void testNothingOverThresholdIsHealthy() {
        RuleResult result = evaluate(IoHealthSnapshot.builder()
                .maxInFlightNanos(TimeUnit.MILLISECONDS.toNanos(HANG)).build());
        Assert.assertFalse(result.isContainerAnomalous());
        Assert.assertTrue(result.triggered().isEmpty());
    }

    private RuleResult evaluate(IoHealthSnapshot snapshot) {
        RuleResult result = new RuleResult();
        rule.evaluate(new RuleContext(snapshot, RuleConfig.builder().build(), 0L), result);
        return result;
    }
}
