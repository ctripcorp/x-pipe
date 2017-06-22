package com.ctrip.xpipe.redis.console.controller.migrate.meta;

/**
 * @author wenchao.meng
 *         <p>
 *         Jun 22, 2017
 */
public class AbstractClusterMeta extends AbstractMeta {

    private String clusterName;

    public AbstractClusterMeta(){
    }

    public AbstractClusterMeta(String clusterName){
        this.clusterName = clusterName;
    }

    public String getClusterName() {
        return clusterName;
    }

    public void setClusterName(String clusterName) {
        this.clusterName = clusterName;
    }
}