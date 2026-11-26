package com.ctrip.xpipe.redis.console.service.logicalbu;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

/** Result of one logical BU. Each BU succeeds or fails on its own. */
public class LogicalBuOpResult {

    public enum State {
        /** Nothing changed: conflict, BU not found, or data could not be loaded. */
        BLOCKED,
        SUCCESS,
        /** Failed at or after the DB update (DB / notify). */
        FAILED
    }

    private long logicalBuId;
    private String logicalBuName;
    private State state;
    private OpStage failedStage;
    private Integer priorityBefore;
    private Integer priorityAfter;
    /** IDCs hosting this BU's TFS keepercontainers, i.e. the IDCs whose MetaServers were asked to reload. */
    private Set<String> idcs = new TreeSet<>();
    /** resetHealth only: keepers asked to recover / keepers that answered OK */
    private Integer keeperCount;
    private Integer successKeeperCount;
    /** resetHealth: TFS keepercontainers of the BU */
    private Integer containerCount;
    /** resetHealth: containers whose keeper list loaded and every keeper recovered */
    private Integer successContainerCount;
    private List<OpFailure> failures = new ArrayList<>();

    public LogicalBuOpResult() {
    }

    public LogicalBuOpResult(long logicalBuId) {
        this.logicalBuId = logicalBuId;
    }

    public void addFailure(OpFailure failure) {
        failures.add(failure);
        if (failedStage == null || failure.getStage().ordinal() < failedStage.ordinal()) {
            failedStage = failure.getStage();
        }
    }

    public void finish(State finalState) {
        this.state = finalState;
    }

    public boolean hasFailures() {
        return !failures.isEmpty();
    }

    public long getLogicalBuId() {
        return logicalBuId;
    }

    public String getLogicalBuName() {
        return logicalBuName;
    }

    public LogicalBuOpResult setLogicalBuName(String logicalBuName) {
        this.logicalBuName = logicalBuName;
        return this;
    }

    public State getState() {
        return state;
    }

    public OpStage getFailedStage() {
        return failedStage;
    }

    public Integer getPriorityBefore() {
        return priorityBefore;
    }

    public LogicalBuOpResult setPriorityBefore(Integer priorityBefore) {
        this.priorityBefore = priorityBefore;
        return this;
    }

    public Integer getPriorityAfter() {
        return priorityAfter;
    }

    public LogicalBuOpResult setPriorityAfter(Integer priorityAfter) {
        this.priorityAfter = priorityAfter;
        return this;
    }

    public Set<String> getIdcs() {
        return idcs;
    }

    public Integer getKeeperCount() {
        return keeperCount;
    }

    public LogicalBuOpResult setKeeperCount(Integer keeperCount) {
        this.keeperCount = keeperCount;
        return this;
    }

    public Integer getSuccessKeeperCount() {
        return successKeeperCount;
    }

    public LogicalBuOpResult setSuccessKeeperCount(Integer successKeeperCount) {
        this.successKeeperCount = successKeeperCount;
        return this;
    }

    public Integer getContainerCount() {
        return containerCount;
    }

    public LogicalBuOpResult setContainerCount(Integer containerCount) {
        this.containerCount = containerCount;
        return this;
    }

    public Integer getSuccessContainerCount() {
        return successContainerCount;
    }

    public LogicalBuOpResult setSuccessContainerCount(Integer successContainerCount) {
        this.successContainerCount = successContainerCount;
        return this;
    }

    public List<OpFailure> getFailures() {
        return failures;
    }
}
