package com.ctrip.xpipe.redis.keeper.health.rule;

import com.ctrip.xpipe.redis.keeper.storage.health.IoHealthSnapshot;

public final class HangRule implements IoHealthRule {

    public static final String NAME = "hang";

    @Override
    public String name() {
        return NAME;
    }

    @Override
    public void evaluate(RuleContext ctx, RuleResult result) {
        IoHealthSnapshot snapshot = ctx.snapshot();
        long hangNanos = ctx.config().hangNanos();
        if (snapshot.business().maxInFlightNanos() > hangNanos
                || snapshot.business().maxExecNanos() > hangNanos) {
            result.markContainer(this);
        }
        if (snapshot.probe().tasks().maxInFlightNanos() > hangNanos
                || snapshot.probe().tasks().maxExecNanos() > hangNanos) {
            result.markContainer(this);
        }
    }
}
