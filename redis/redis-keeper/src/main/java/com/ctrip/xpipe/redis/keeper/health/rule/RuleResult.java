package com.ctrip.xpipe.redis.keeper.health.rule;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

public final class RuleResult {

    private boolean containerAnomalous;
    private final Set<String> anomalousTenants = new LinkedHashSet<>();
    private final List<RuleTrigger> triggerDetails = new ArrayList<>();

    public void markContainer(IoHealthRule rule, RuleTrigger trigger) {
        containerAnomalous = true;
        addTrigger(rule, RuleTrigger.Scope.CONTAINER, null, trigger);
    }

    public void markTenant(IoHealthRule rule, String tenant, RuleTrigger trigger) {
        anomalousTenants.add(tenant);
        addTrigger(rule, RuleTrigger.Scope.TENANT, tenant, trigger);
    }

    private void addTrigger(IoHealthRule rule, RuleTrigger.Scope expectedScope,
            String expectedTenant, RuleTrigger trigger) {
        Objects.requireNonNull(trigger, "trigger");
        if (!rule.name().equals(trigger.rule())) {
            throw new IllegalArgumentException("trigger rule mismatch: " + trigger);
        }
        if (trigger.scope() != expectedScope) {
            throw new IllegalArgumentException("trigger scope mismatch: " + trigger);
        }
        if (expectedScope == RuleTrigger.Scope.TENANT
                && !Objects.equals(expectedTenant, trigger.tenant())) {
            throw new IllegalArgumentException("trigger tenant mismatch: " + trigger);
        }
        triggerDetails.add(trigger);
    }

    public boolean isContainerAnomalous() {
        return containerAnomalous;
    }

    public boolean isAnomalous(String tenant) {
        return containerAnomalous || anomalousTenants.contains(tenant);
    }

    public Set<String> anomalousTenants() {
        return Collections.unmodifiableSet(anomalousTenants);
    }

    /** All source-specific evidence. BUSINESS and PROBE entries never overwrite each other. */
    public List<RuleTrigger> triggerDetails() {
        return Collections.unmodifiableList(triggerDetails);
    }
}
