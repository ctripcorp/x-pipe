package com.ctrip.xpipe.redis.keeper.health.rule;

import com.ctrip.xpipe.redis.keeper.storage.health.IoHealthSnapshot;
import org.junit.Assert;
import org.junit.Test;

public class TenantAbnormalRuleTest {

    private final TenantAbnormalRule rule = new TenantAbnormalRule();

    @Test
    public void testOnlyTheBadShardIsMarked() {
        // 6 abnormal over 100 known -> above max(5%, 5)
        IoHealthSnapshot snapshot = IoHealthSnapshot.builder()
                .tenant("repl_1", 100, 6, 0, 0)
                .tenant("repl_2", 100, 0, 0, 0)
                .build();
        RuleResult result = evaluate(snapshot);
        Assert.assertTrue(result.isAnomalous("repl_1"));
        Assert.assertFalse(result.isAnomalous("repl_2"));
    }

    @Test
    public void testFloorProtectsAnIdleShard() {
        // 5 abnormal is not above the floor of 5
        Assert.assertFalse(evaluate(IoHealthSnapshot.builder().tenant("repl_1", 0, 5, 0, 0).build())
                .isAnomalous("repl_1"));

        // 6 is
        Assert.assertTrue(evaluate(IoHealthSnapshot.builder().tenant("repl_1", 0, 6, 0, 0).build())
                .isAnomalous("repl_1"));
    }

    @Test
    public void testRunningSlowTasksCount() {
        // 2 finished abnormal + 4 running-slow = 6, over known = 100 + 4
        IoHealthSnapshot snapshot = IoHealthSnapshot.builder()
                .tenant("repl_1", 100, 2, 10, 4)
                .build();
        Assert.assertTrue(evaluate(snapshot).isAnomalous("repl_1"));
    }

    private RuleResult evaluate(IoHealthSnapshot snapshot) {
        RuleResult result = new RuleResult();
        rule.evaluate(new RuleContext(snapshot, RuleConfig.builder().build(), 0L), result);
        return result;
    }
}
