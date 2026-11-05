package com.ctrip.xpipe.redis.keeper.health.rule;

import com.ctrip.xpipe.redis.keeper.storage.health.IoHealthSnapshot;
import org.junit.Assert;
import org.junit.Test;

public class FailRatioRuleTest {

    private final FailRatioRule rule = new FailRatioRule();

    @Test
    public void testBelowSampleFloorIsNotJudged() {
        // 100% of a tiny sample must not degrade anybody ("30s没到1000就不判断")
        RuleResult result = evaluate(IoHealthSnapshot.builder().total(60).failed(60).build());
        Assert.assertFalse(result.isContainerAnomalous());
    }

    @Test
    public void testRatioAbovePercentFires() {
        RuleResult result = evaluate(IoHealthSnapshot.builder().total(1000).failed(11).build());
        Assert.assertTrue(result.isContainerAnomalous());
        Assert.assertTrue(result.triggered().contains(FailRatioRule.NAME));
    }

    @Test
    public void testExactlyAtPercentDoesNotFire() {
        // 10/1000 == 1%, the rule is strictly greater
        RuleResult result = evaluate(IoHealthSnapshot.builder().total(1000).failed(10).build());
        Assert.assertFalse(result.isContainerAnomalous());
    }

    private RuleResult evaluate(IoHealthSnapshot snapshot) {
        RuleResult result = new RuleResult();
        rule.evaluate(new RuleContext(snapshot, RuleConfig.builder().build(), 0L), result);
        return result;
    }
}
