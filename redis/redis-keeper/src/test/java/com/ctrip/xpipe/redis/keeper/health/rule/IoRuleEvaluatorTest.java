package com.ctrip.xpipe.redis.keeper.health.rule;

import com.ctrip.xpipe.redis.keeper.storage.health.IoHealthSnapshot;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class IoRuleEvaluatorTest {

    @Test
    public void testDefaultsCoverAllFiveRules() {
        List<String> names = IoRuleEvaluator.defaults().rules().stream()
                .map(IoHealthRule::name).collect(java.util.stream.Collectors.toList());
        Assert.assertTrue(names.contains(HangRule.NAME));
        Assert.assertTrue(names.contains(FailRatioRule.NAME));
        Assert.assertTrue(names.contains(SlowRatioRule.NAME));
        Assert.assertTrue(names.contains(TenantAbnormalRule.NAME));
        Assert.assertTrue(names.contains(FileFailureRule.NAME));
    }

    @Test
    public void testDisabledRuleIsSkipped() {
        IoHealthSnapshot hanging = IoHealthSnapshot.builder()
                .maxInFlightNanos(TimeUnit.SECONDS.toNanos(10)).build();
        RuleConfig disabled = RuleConfig.builder().disabledRules(HangRule.NAME).build();

        RuleResult result = IoRuleEvaluator.defaults()
                .evaluate(new RuleContext(hanging, disabled, 0L));

        Assert.assertFalse(result.isContainerAnomalous());
        Assert.assertTrue(result.triggered().isEmpty());
    }

    @Test
    public void testContainerRuleMarksEveryTenant() {
        IoHealthSnapshot snapshot = IoHealthSnapshot.builder()
                .maxInFlightNanos(TimeUnit.SECONDS.toNanos(10))
                .tenant("repl_1", 100, 0, 0, 0)
                .tenant("repl_2", 100, 0, 0, 0)
                .build();
        RuleResult result = IoRuleEvaluator.defaults()
                .evaluate(new RuleContext(snapshot, RuleConfig.builder().build(), 0L));

        Assert.assertTrue(result.isAnomalous("repl_1"));
        Assert.assertTrue(result.isAnomalous("repl_2"));
    }

    @Test
    public void testResultDoesNotDependOnRuleOrder() {
        // only the tenant rule fires; reversing the list must not change the outcome
        IoHealthSnapshot snapshot = IoHealthSnapshot.builder()
                .tenant("repl_1", 100, 6, 0, 0)
                .tenant("repl_2", 100, 0, 0, 0)
                .build();
        RuleConfig config = RuleConfig.builder().build();

        List<IoHealthRule> forward = Arrays.asList(
                new HangRule(), new FailRatioRule(), new SlowRatioRule(),
                new TenantAbnormalRule(), new FileFailureRule());
        List<IoHealthRule> reversed = Arrays.asList(
                new FileFailureRule(), new TenantAbnormalRule(), new SlowRatioRule(),
                new FailRatioRule(), new HangRule());

        RuleResult a = new IoRuleEvaluator(forward).evaluate(new RuleContext(snapshot, config, 0L));
        RuleResult b = new IoRuleEvaluator(reversed).evaluate(new RuleContext(snapshot, config, 0L));

        Assert.assertEquals(a.anomalousTenants(), b.anomalousTenants());
        Assert.assertEquals(a.isContainerAnomalous(), b.isContainerAnomalous());
        Assert.assertTrue(a.isAnomalous("repl_1"));
        Assert.assertFalse(a.isAnomalous("repl_2"));
    }

    @Test
    public void testTriggeredCollectsEveryRuleThatFired() {
        IoHealthSnapshot snapshot = IoHealthSnapshot.builder()
                .total(1000).failed(100)
                .maxExecNanos(TimeUnit.SECONDS.toNanos(10))
                .build();
        RuleResult result = IoRuleEvaluator.defaults()
                .evaluate(new RuleContext(snapshot, RuleConfig.builder().build(), 0L));

        Assert.assertTrue(result.triggered().contains(HangRule.NAME));
        Assert.assertTrue(result.triggered().contains(FailRatioRule.NAME));
    }
}
