package com.ctrip.xpipe.endpoint;

import com.ctrip.xpipe.utils.ObjectUtils;

/**
 * @author wenchao.meng
 *         <p>
 *         Aug 21, 2017
 */
public class ClusterShardHostPort {

    private String clusterName;

    private String shardName;

    private HostPort hostPort;


    public ClusterShardHostPort(String clusterName, String shardName, HostPort hostPort){
        this.clusterName = clusterName;
        this.shardName = shardName;
        this.hostPort = hostPort;
    }

    public ClusterShardHostPort(String clusterName, String shardName){
        this.clusterName = clusterName;
        this.shardName = shardName;
    }

    public ClusterShardHostPort(HostPort hostPort){
        this.hostPort = hostPort;
    }

    public HostPort getHostPort() {
        return hostPort;
    }

    public void setHostPort(HostPort hostPort) {
        this.hostPort = hostPort;
    }

    public String getClusterName() {
        return clusterName;
    }

    public String getShardName() {
        return shardName;
    }

    public void setClusterName(String clusterName) {
        this.clusterName = clusterName;
    }

    public void setShardName(String shardName) {
        this.shardName = shardName;
    }

    @Override
    public boolean equals(Object obj) {

        if(this == obj){
            return true;
        }

        if(!(obj instanceof ClusterShardHostPort)){
            return false;
        }

        ClusterShardHostPort other = (ClusterShardHostPort) obj;
        if(!(ObjectUtils.equals(this.clusterName, other.clusterName))){
            return false;
        }
        if(!(ObjectUtils.equals(this.shardName, other.shardName))){
            return false;
        }
        if(!(ObjectUtils.equals(this.hostPort, other.hostPort))){
            return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        return ObjectUtils.hashCode(clusterName, shardName, hostPort);
    }

    @Override
    public String toString() {
        return String.format("%s,%s[%s]", clusterName, shardName, hostPort);
    }
}
