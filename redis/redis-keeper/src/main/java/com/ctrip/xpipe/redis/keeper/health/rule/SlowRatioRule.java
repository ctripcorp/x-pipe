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
        IoHealthSnapshot.SourceSnapshot business = ctx.snapshot().business();
        if (business.total() >= ctx.config().minWindowRecords()) {
            long slow = business.slow() + business.slowInFlight();
            long known = business.total() + business.slowInFlight();
            if (slow * 100L >= known * ctx.config().slowRatioPercent()) {
                result.markContainer(this);
            }
        }

        IoHealthSnapshot.ProbeSnapshot probe = ctx.snapshot().probe();
        if (probe.rounds() >= ctx.config().probeSlowMinRecords()
                && probe.slowRounds() * 100L
                >= probe.rounds() * ctx.config().probeSlowRatioPercent()) {
            result.markContainer(this);
        }
    }
}
