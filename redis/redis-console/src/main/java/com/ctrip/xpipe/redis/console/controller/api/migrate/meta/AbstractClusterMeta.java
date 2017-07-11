package com.ctrip.xpipe.redis.console.controller.api.migrate.meta;

/**
 * @author wenchao.meng
 *         <p>
 *         Jun 22, 2017
 */
public class AbstractClusterMeta extends AbstractMeta {

    private String clusterName;
    private String msg;

    private String fromIdc;
    private String toIdc;


    public AbstractClusterMeta(){
    }

    public AbstractClusterMeta(String clusterName){
        this.clusterName = clusterName;
    }

    public AbstractClusterMeta(String clusterName, String fromIdc, String toIdc){
        this.clusterName = clusterName;
        this.fromIdc = fromIdc;
        this.toIdc = toIdc;
    }

    public AbstractClusterMeta(String clusterName, String fromIdc, String toIdc, String msg){
        this.clusterName = clusterName;
        this.fromIdc = fromIdc;
        this.toIdc = toIdc;
        this.msg = msg;

    }


    public AbstractClusterMeta(String clusterName, String msg){
        this.clusterName = clusterName;
        this.msg = msg;
    }

    public String getClusterName() {
        return clusterName;
    }

    public void setClusterName(String clusterName) {
        this.clusterName = clusterName;
    }

    public String getMsg() {
        return msg;
    }

    public void setMsg(String msg) {
        this.msg = msg;
    }

    public String getFromIdc() {
        return fromIdc;
    }

    public void setFromIdc(String fromIdc) {
        this.fromIdc = fromIdc;
    }

    public String getToIdc() {
        return toIdc;
    }

    public void setToIdc(String toIdc) {
        this.toIdc = toIdc;
    }
}