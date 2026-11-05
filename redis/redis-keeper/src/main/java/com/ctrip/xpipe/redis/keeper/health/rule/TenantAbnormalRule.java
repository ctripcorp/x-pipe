package com.ctrip.xpipe.redis.keeper.health.rule;

import com.ctrip.xpipe.redis.keeper.storage.health.IoHealthSnapshot;

import java.util.Map;

public final class TenantAbnormalRule implements IoHealthRule {

    public static final String NAME = "tenantAbnormal";

    @Override
    public String name() {
        return NAME;
    }

    @Override
    public void evaluate(RuleContext ctx, RuleResult result) {
        for (Map.Entry<String, IoHealthSnapshot.TenantStat> e : ctx.snapshot().tenants().entrySet()) {
            IoHealthSnapshot.TenantStat stat = e.getValue();
            long abnormal = stat.abnormal() + stat.slowInFlight();
            long known = stat.total() + stat.slowInFlight();
            long threshold = Math.max(ctx.config().tenantAbnormalFloor(),
                    known * ctx.config().tenantAbnormalPercent() / 100L);
            if (abnormal > threshold) {
                result.markTenant(this, e.getKey());
            }
        }
    }
}
