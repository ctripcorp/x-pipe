package com.ctrip.xpipe.redis.keeper.health.rule;

import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Set;

public final class RuleResult {

    private boolean containerAnomalous;
    private final Set<String> anomalousTenants = new LinkedHashSet<>();
    private final Set<String> triggered = new LinkedHashSet<>();

    public void markContainer(IoHealthRule rule) {
        containerAnomalous = true;
        triggered.add(rule.name());
    }

    public void markTenant(IoHealthRule rule, String tenant) {
        anomalousTenants.add(tenant);
        triggered.add(rule.name());
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

    public Set<String> triggered() {
        return Collections.unmodifiableSet(triggered);
    }
}
