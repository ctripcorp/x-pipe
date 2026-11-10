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
        IoHealthSnapshot.SourceSnapshot business = ctx.snapshot().business();
        if (business.total() < ctx.config().minWindowRecords()) {
            return;
        }
        if (business.failed() * 100L > business.total() * ctx.config().failRatioPercent()) {
            result.markContainer(this);
        }
    }
}
