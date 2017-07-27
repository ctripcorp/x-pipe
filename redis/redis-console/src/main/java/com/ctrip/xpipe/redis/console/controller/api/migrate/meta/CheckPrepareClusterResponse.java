package com.ctrip.xpipe.redis.console.controller.api.migrate.meta;

/**
 * @author wenchao.meng
 *         <p>
 *         Jun 22, 2017
 */
public class CheckPrepareClusterResponse extends AbstractClusterMeta{

    private CHECK_FAIL_STATUS failReason;
    private boolean success;

    public CheckPrepareClusterResponse() {

    }

    private CheckPrepareClusterResponse(String clusterName, String fromIdc, String toIdc) {
        super(clusterName, fromIdc, toIdc);
        this.success = true;

    }

    private CheckPrepareClusterResponse(String clusterName, String fromIdc, CHECK_FAIL_STATUS failReason, String msg) {
        super(clusterName, msg);
        setFromIdc(fromIdc);
        this.success = false;
        this.failReason = failReason;
    }

    private CheckPrepareClusterResponse(String clusterName, String fromIdc,String toIdc, CHECK_FAIL_STATUS failReason, String msg) {
        super(clusterName, msg);
        setFromIdc(fromIdc);
        this.setToIdc(toIdc);
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

    public static CheckPrepareClusterResponse createSuccessResponse(String clusterName, String fromIdc, String toIdc) {
        return new CheckPrepareClusterResponse(clusterName, fromIdc, toIdc);
    }

    public static CheckPrepareClusterResponse createFailResponse(String clusterName, String fromIdc, CHECK_FAIL_STATUS failReason, String msg) {
        return new CheckPrepareClusterResponse(clusterName, fromIdc, failReason, msg);
    }

    public static CheckPrepareClusterResponse createFailResponse(String clusterName, String fromIdc, String toIdc, CHECK_FAIL_STATUS failReason, String msg) {
        return new CheckPrepareClusterResponse(clusterName, fromIdc, toIdc, failReason, msg);
    }

}
