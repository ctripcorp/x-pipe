package com.ctrip.xpipe.redis.checker.model;

import com.ctrip.xpipe.endpoint.HostPort;

import java.util.Objects;

/**
 * @author lishanglin
 * date 2021/3/16
 */
public class CheckerStatus {

    HostPort hostPort;

    CheckerRole checkerRole;
    
    CheckerRole allCheckerRole;

    long lastAckTime;

    int partIndex;

    public HostPort getHostPort() {
        return hostPort;
    }

    public void setHostPort(HostPort hostPort) {
        this.hostPort = hostPort;
    }

    public CheckerRole getCheckerRole() {
        return checkerRole;
    }

    public void setCheckerRole(CheckerRole checkerRole) {
        this.checkerRole = checkerRole;
    }

    public CheckerRole getAllCheckerRole() {
        return allCheckerRole;
    }

    public void setAllCheckerRole(CheckerRole role) {
        this.allCheckerRole = role;
    }

    public long getLastAckTime() {
        return lastAckTime;
    }

    public void setLastAckTime(long lastAckTime) {
        this.lastAckTime = lastAckTime;
    }

    public int getPartIndex() {
        return partIndex;
    }

    public void setPartIndex(int partIndex) {
        this.partIndex = partIndex;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        CheckerStatus that = (CheckerStatus) o;
        return Objects.equals(hostPort, that.hostPort);
    }

    @Override
    public int hashCode() {
        return Objects.hash(hostPort);
    }

    @Override
    public String toString() {
        return "CheckerStatus{" +
                "hostPort=" + hostPort +
                ", checkerRole=" + checkerRole +
                ", lastAckTime=" + lastAckTime +
                ", partIndex=" + partIndex +
                '}';
    }
}
