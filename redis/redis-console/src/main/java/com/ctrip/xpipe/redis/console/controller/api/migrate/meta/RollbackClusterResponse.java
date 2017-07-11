package com.ctrip.xpipe.redis.console.controller.api.migrate.meta;

/**
 * @author wenchao.meng
 *         <p>
 *         Jul 03, 2017
 */
public class RollbackClusterResponse extends AbstractClusterMeta{


    private boolean success;


    public RollbackClusterResponse(boolean result, String clusterName, String fromDc, String toDc, String msg){
        super(clusterName, fromDc, toDc, msg);
        this.success = result;
    }

    public RollbackClusterResponse(boolean result, String clusterName, String msg){
        super(clusterName, null, null, msg);
        this.success = result;
    }

    public boolean isSuccess() {
        return success;
    }


}
