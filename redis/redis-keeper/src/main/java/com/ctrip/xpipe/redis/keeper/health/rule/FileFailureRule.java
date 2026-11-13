package com.ctrip.xpipe.redis.keeper.health.rule;

import com.ctrip.xpipe.redis.keeper.storage.health.IoHealthSnapshot;
import com.ctrip.xpipe.redis.keeper.storage.health.IoSource;

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
        for (Map.Entry<String, IoHealthSnapshot.TenantStat> tenant
                : ctx.snapshot().tenants().entrySet()) {
            for (IoHealthSnapshot.FileStat file : tenant.getValue().files().values()) {
                if (markBusinessFile(ctx, result, tenant.getKey(), file)) {
                    break;
                }
            }
        }
        for (IoHealthSnapshot.FileStat file : ctx.snapshot().probe().files().values()) {
            if (markProbeFile(ctx, result, file)) {
                break;
            }
        }
    }

    private boolean markBusinessFile(RuleContext ctx, RuleResult result, String tenant,
            IoHealthSnapshot.FileStat file) {
        boolean triggered = false;
        if (file.consecutiveFails() >= ctx.config().fileConsecutiveFails()) {
            result.markTenant(this, tenant, RuleTrigger.tenant(
                    this, IoSource.BUSINESS, tenant, file.path(),
                    RuleTrigger.Signal.FILE_CONSECUTIVE_FAILURES,
                    file.consecutiveFails(), ctx.config().fileConsecutiveFails(), 0));
            triggered = true;
        }
        if (file.fails10() >= ctx.config().fileFailsInWindow()) {
            result.markTenant(this, tenant, RuleTrigger.tenant(
                    this, IoSource.BUSINESS, tenant, file.path(),
                    RuleTrigger.Signal.FILE_FAILURES_IN_WINDOW,
                    file.fails10(), ctx.config().fileFailsInWindow(), 0));
            triggered = true;
        }
        return triggered;
    }

    private boolean markProbeFile(RuleContext ctx, RuleResult result,
            IoHealthSnapshot.FileStat file) {
        boolean triggered = false;
        if (file.consecutiveFails() >= ctx.config().fileConsecutiveFails()) {
            result.markContainer(this, RuleTrigger.container(
                    this, IoSource.PROBE, null, file.path(),
                    RuleTrigger.Signal.FILE_CONSECUTIVE_FAILURES,
                    file.consecutiveFails(), ctx.config().fileConsecutiveFails(), 0));
            triggered = true;
        }
        if (file.fails10() >= ctx.config().fileFailsInWindow()) {
            result.markContainer(this, RuleTrigger.container(
                    this, IoSource.PROBE, null, file.path(),
                    RuleTrigger.Signal.FILE_FAILURES_IN_WINDOW,
                    file.fails10(), ctx.config().fileFailsInWindow(), 0));
            triggered = true;
        }
        return triggered;
    }
}
