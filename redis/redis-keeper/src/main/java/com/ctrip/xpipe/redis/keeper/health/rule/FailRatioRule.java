package com.ctrip.xpipe.redis.keeper.health.rule;

import com.ctrip.xpipe.redis.keeper.storage.health.IoHealthSnapshot;

public final class FailRatioRule implements IoHealthRule {

    public static final String NAME = "failRatio";

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
        if (snapshot.failed() * 100L > snapshot.total() * ctx.config().failRatioPercent()) {
            result.markContainer(this);
        }
    }
}
