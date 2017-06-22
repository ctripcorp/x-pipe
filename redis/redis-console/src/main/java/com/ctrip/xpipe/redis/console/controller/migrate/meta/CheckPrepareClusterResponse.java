package com.ctrip.xpipe.redis.console.controller.migrate.meta;

/**
 * @author wenchao.meng
 *         <p>
 *         Jun 22, 2017
 */
public class CheckPrepareClusterResponse extends AbstractClusterMeta{

    private String clusterName;
    private CHECK_FAIL_STATUS failReason;
    private boolean success;

    public CheckPrepareClusterResponse() {

    }

    private CheckPrepareClusterResponse(String clusterName) {
        super(clusterName);
        this.success = true;


    }

    private CheckPrepareClusterResponse(String clusterName, CHECK_FAIL_STATUS failReason, String msg) {
        super(clusterName, msg);
        this.success = false;
        this.failReason = failReason;
    }

    public boolean isSuccess() {
        return success;
    }

    public void setSuccess(boolean success) {
        this.success = success;
    }

    public CHECK_FAIL_STATUS getFailReason() {
        return failReason;
    }

    public void setFailReason(CHECK_FAIL_STATUS failReason) {
        this.failReason = failReason;
    }

    public static CheckPrepareClusterResponse createSuccessResponse(String clusterName) {
        return new CheckPrepareClusterResponse(clusterName);
    }

    public static CheckPrepareClusterResponse createFailResponse(String clusterName, CHECK_FAIL_STATUS failReason, String msg) {
        return new CheckPrepareClusterResponse(clusterName, failReason, msg);
    }
}
