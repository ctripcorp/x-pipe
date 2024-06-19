package com.ctrip.xpipe.redis.checker.healthcheck.actions.sentinel;

import com.ctrip.xpipe.endpoint.HostPort;

import java.util.HashSet;
import java.util.Set;

public class SentinelInvalidSlaves {

    private Set<HostPort> tooManyKeepers = new HashSet<>();
    private Set<HostPort> unknownSlaves = new HashSet<>();

    public SentinelInvalidSlaves() {
    }

    public SentinelInvalidSlaves(Set<HostPort> tooManyKeepers, Set<HostPort> unknownSlaves) {
        this.tooManyKeepers = tooManyKeepers;
        this.unknownSlaves = unknownSlaves;
    }

    public Set<HostPort> getTooManyKeepers() {
        return tooManyKeepers;
    }

    public void setTooManyKeepers(Set<HostPort> tooManyKeepers) {
        this.tooManyKeepers = tooManyKeepers;
    }

    public Set<HostPort> getUnknownSlaves() {
        return unknownSlaves;
    }

    public void setUnknownSlaves(Set<HostPort> unknownSlaves) {
        this.unknownSlaves = unknownSlaves;
    }

    public boolean hasInvalidSlaves() {
        return !(tooManyKeepers.isEmpty() && unknownSlaves.isEmpty());
    }
}
