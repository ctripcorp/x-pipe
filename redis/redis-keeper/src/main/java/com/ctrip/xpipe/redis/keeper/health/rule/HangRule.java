package com.ctrip.xpipe.redis.keeper.health.rule;

import com.ctrip.xpipe.redis.keeper.storage.health.IoHealthSnapshot;
import com.ctrip.xpipe.redis.keeper.storage.health.IoSource;

public final class HangRule implements IoHealthRule {

    public static final String NAME = "hang";

    @Override
    public String name() {
        return NAME;
    }

    @Override
    public void evaluate(RuleContext ctx, RuleResult result) {
        long hangNanos = ctx.config().hangNanos();
        markSource(result, IoSource.BUSINESS, ctx.snapshot().business(), hangNanos);
        markSource(result, IoSource.PROBE, ctx.snapshot().probe().tasks(), hangNanos);
    }

    private void markSource(RuleResult result, IoSource source,
            IoHealthSnapshot.SourceSnapshot snapshot, long hangNanos) {
        if (snapshot.maxInFlightNanos() > hangNanos) {
            result.markContainer(this, RuleTrigger.container(
                    this, source, snapshot.maxInFlightTenant(), snapshot.maxInFlightPath(),
                    RuleTrigger.Signal.IN_FLIGHT_NANOS,
                    snapshot.maxInFlightNanos(), hangNanos, snapshot.inFlight()));
        }
        if (snapshot.maxExecNanos() > hangNanos) {
            result.markContainer(this, RuleTrigger.container(
                    this, source, snapshot.maxExecTenant(), snapshot.maxExecPath(),
                    RuleTrigger.Signal.MAX_EXEC_NANOS,
                    snapshot.maxExecNanos(), hangNanos, snapshot.total()));
        }
    }
}
