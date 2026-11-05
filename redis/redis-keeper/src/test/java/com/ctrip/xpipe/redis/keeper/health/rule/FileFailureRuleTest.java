package com.ctrip.xpipe.redis.keeper.health.rule;

import com.ctrip.xpipe.redis.keeper.storage.health.IoHealthSnapshot;
import org.junit.Assert;
import org.junit.Test;

public class FileFailureRuleTest {

    private final FileFailureRule rule = new FileFailureRule();

    @Test
    public void testConsecutiveFailuresMarkTheOwningKeeper() {
        RuleResult result = evaluate(IoHealthSnapshot.builder()
                .file("repl_1", "/data/bad", 3, 3, 0L).build());
        Assert.assertTrue(result.isAnomalous("repl_1"));
        Assert.assertTrue(result.triggered().contains(FileFailureRule.NAME));
    }

    @Test
    public void testFailuresInWindowMarkTheOwningKeeper() {
        RuleResult result = evaluate(IoHealthSnapshot.builder()
                .file("repl_1", "/data/bad", 0, 5, 0L).build());
        Assert.assertTrue(result.isAnomalous("repl_1"));
    }

    @Test
    public void testBelowBothThresholdsIsNotAMarked() {
        RuleResult result = evaluate(IoHealthSnapshot.builder()
                .file("repl_1", "/data/bad", 2, 4, 0L).build());
        Assert.assertFalse(result.isAnomalous("repl_1"));
        Assert.assertTrue(result.triggered().isEmpty());
    }

    @Test
    public void testTenantComesFromTheFileNotFromTheKey() {
        IoHealthSnapshot snapshot = IoHealthSnapshot.builder()
                .file("repl_7", "/data/bad", 3, 3, 0L).build();
        RuleResult result = evaluate(snapshot);
        Assert.assertTrue(result.isAnomalous("repl_7"));
        Assert.assertFalse(result.isAnomalous("repl_1"));
    }

    private RuleResult evaluate(IoHealthSnapshot snapshot) {
        RuleResult result = new RuleResult();
        rule.evaluate(new RuleContext(snapshot, RuleConfig.builder().build(), 0L), result);
        return result;
    }
}
