package com.ctrip.xpipe.redis.console.controller.migrate.meta;

/**
 * @author wenchao.meng
 *         <p>
 *         Jun 22, 2017
 */
public class AbstractClusterMeta extends AbstractMeta {

    private String clusterName;
    private String msg;


    public AbstractClusterMeta(){
    }

    public AbstractClusterMeta(String clusterName){
        this.clusterName = clusterName;
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


}