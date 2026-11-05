package com.ctrip.xpipe.redis.keeper.health.rule;

public interface IoHealthRule {

    String name();

    void evaluate(RuleContext ctx, RuleResult result);
}
