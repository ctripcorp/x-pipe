package com.ctrip.xpipe.redis.console.migration.model;

/**
 * @author wenchao.meng
 *         <p>
 *         Aug 03, 2017
 */
public class ClusterStepResult {

    private final int totalCount;
    private final int finishCount;
    private final int successCount;

    public ClusterStepResult(int totalCount, int finishCount, int successCount){
        this.totalCount = totalCount;
        this.finishCount = finishCount;
        this.successCount = successCount;
    }

    public boolean isStepFinish() {
        return finishCount == totalCount;
    }

    public boolean isStepSuccess() {
        return successCount == totalCount;
    }

}
