package com.ctrip.xpipe.redis.keeper.health.rule;

import com.ctrip.xpipe.redis.keeper.storage.health.IoHealthSnapshot;
import org.junit.Assert;
import org.junit.Test;

public class SlowRatioRuleTest {

    private final SlowRatioRule rule = new SlowRatioRule();

    @Test
    public void testBelowSampleFloorIsNotJudged() {
        RuleResult result = evaluate(IoHealthSnapshot.builder().total(60).slow(60).build());
        Assert.assertFalse(result.isContainerAnomalous());
    }

    @Test
    public void testRatioAtOrAbovePercentFires() {
        // 50/1000 == 5%, the rule is greater-or-equal
        RuleResult result = evaluate(IoHealthSnapshot.builder().total(1000).slow(50).build());
        Assert.assertTrue(result.isContainerAnomalous());
        Assert.assertTrue(result.triggered().contains(SlowRatioRule.NAME));
    }

    @Test
    public void testRunningSlowTasksCountOnBothSides() {
        // 45 finished-slow + 6 running-slow over (1000 finished + 6 running-slow) = 51/1006 >= 5%
        RuleResult result = evaluate(IoHealthSnapshot.builder()
                .total(1000).slow(45).inFlight(20).slowInFlight(6).build());
        Assert.assertTrue(result.isContainerAnomalous());
    }

    @Test
    public void testHealthyRatioDoesNotFire() {
        RuleResult result = evaluate(IoHealthSnapshot.builder().total(1000).slow(49).build());
        Assert.assertFalse(result.isContainerAnomalous());
    }

    private RuleResult evaluate(IoHealthSnapshot snapshot) {
        RuleResult result = new RuleResult();
        rule.evaluate(new RuleContext(snapshot, RuleConfig.builder().build(), 0L), result);
        return result;
    }
}
