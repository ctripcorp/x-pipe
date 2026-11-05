package com.ctrip.xpipe.redis.keeper.health.rule;

import com.ctrip.xpipe.redis.keeper.storage.health.IoHealthSnapshot;

public final class SlowRatioRule implements IoHealthRule {

    public static final String NAME = "slowRatio";

    @Override
    public String name() {
        return NAME;
    }

    @Override
    public void evaluate(RuleContext ctx, RuleResult result) {
        IoHealthSnapshot snapshot = ctx.snapshot();
        if (snapshot.total() < ctx.config().minWindowRecords()) {
            return;
        }
        long slow = snapshot.slow() + snapshot.slowInFlight();
        long known = snapshot.total() + snapshot.slowInFlight();
        if (slow * 100L >= known * ctx.config().slowRatioPercent()) {
            result.markContainer(this);
        }
    }
}
