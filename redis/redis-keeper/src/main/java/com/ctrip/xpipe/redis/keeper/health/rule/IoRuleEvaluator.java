package com.ctrip.xpipe.redis.keeper.health.rule;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public final class IoRuleEvaluator {

    private final List<IoHealthRule> rules;

    public IoRuleEvaluator(List<IoHealthRule> rules) {
        this.rules = Collections.unmodifiableList(new ArrayList<>(rules));
    }

    public static IoRuleEvaluator defaults() {
        return new IoRuleEvaluator(List.of(
                new HangRule(),
                new FailRatioRule(),
                new SlowRatioRule(),
                new TenantAbnormalRule(),
                new FileFailureRule()));
    }

    public RuleResult evaluate(RuleContext ctx) {
        RuleResult result = new RuleResult();
        for (IoHealthRule rule : rules) {
            if (!ctx.config().isEnabled(rule.name())) {
                continue;
            }
            rule.evaluate(ctx, result);
        }
        return result;
    }

    public List<IoHealthRule> rules() {
        return rules;
    }
}
