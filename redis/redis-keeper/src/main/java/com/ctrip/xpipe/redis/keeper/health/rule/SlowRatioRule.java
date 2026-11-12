package com.ctrip.xpipe.redis.keeper.health.rule;

import com.ctrip.xpipe.redis.keeper.storage.health.IoHealthSnapshot;
import com.ctrip.xpipe.redis.keeper.storage.health.IoSource;

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
                result.markContainer(this, RuleTrigger.container(
                        this, IoSource.BUSINESS, null, RuleTrigger.Signal.SLOW_RATIO,
                        slow, ctx.config().slowRatioPercent(), known));
            }
        }

        IoHealthSnapshot.ProbeSnapshot probe = ctx.snapshot().probe();
        if (probe.rounds() >= ctx.config().probeSlowMinRecords()
                && probe.slowRounds() * 100L
                >= probe.rounds() * ctx.config().probeSlowRatioPercent()) {
            result.markContainer(this, RuleTrigger.container(
                    this, IoSource.PROBE, null, RuleTrigger.Signal.SLOW_RATIO,
                    probe.slowRounds(), ctx.config().probeSlowRatioPercent(), probe.rounds()));
        }
    }
}
