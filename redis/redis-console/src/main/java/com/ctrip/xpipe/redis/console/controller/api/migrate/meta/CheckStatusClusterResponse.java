package com.ctrip.xpipe.redis.console.controller.api.migrate.meta;

/**
 * @author wenchao.meng
 *         <p>
 *         Jun 22, 2017
 */
public class CheckStatusClusterResponse extends AbstractClusterMeta{

    private DO_STATUS status;
    private int percent;

    public CheckStatusClusterResponse(){

    }

    public CheckStatusClusterResponse(String clusterName){
        super(clusterName);
    }

    public CheckStatusClusterResponse(String clusterName, DO_STATUS status, int percent, String msg){
        super(clusterName, msg);
        this.status = status;
        this.percent = percent;
    }


    public DO_STATUS getStatus() {
        return status;
    }

    public void setStatus(DO_STATUS status) {
        this.status = status;
    }

    public int getPercent() {
        return percent;
    }

    public void setPercent(int percent) {
        this.percent = percent;
    }

    public void updateMessage(DO_STATUS status, int percent, String msg){
        setStatus(status);
        setPercent(percent);
        setMsg(msg);
    }
}
