package com.ctrip.xpipe.redis.keeper.health.rule;

import com.ctrip.xpipe.redis.keeper.storage.health.IoHealthSnapshot;

import java.util.Map;

/** Any file of a keeper with consecutive failures, or failures inside the file window, marks it. */
public final class FileFailureRule implements IoHealthRule {

    public static final String NAME = "fileFailure";

    @Override
    public String name() {
        return NAME;
    }

    @Override
    public void evaluate(RuleContext ctx, RuleResult result) {
        for (Map.Entry<String, IoHealthSnapshot.TenantStat> tenant : ctx.snapshot().tenants().entrySet()) {
            for (IoHealthSnapshot.FileStat file : tenant.getValue().files().values()) {
                if (file.consecutiveFails() >= ctx.config().fileConsecutiveFails()
                        || file.fails10() >= ctx.config().fileFailsInWindow()) {
                    result.markTenant(this, tenant.getKey());
                    break;
                }
            }
        }
        for (IoHealthSnapshot.FileStat file : ctx.snapshot().probe().files().values()) {
            if (file.consecutiveFails() >= ctx.config().fileConsecutiveFails()
                    || file.fails10() >= ctx.config().fileFailsInWindow()) {
                result.markContainer(this);
                break;
            }
        }
    }
}
