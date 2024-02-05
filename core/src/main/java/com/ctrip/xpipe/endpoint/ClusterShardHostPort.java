package com.ctrip.xpipe.endpoint;

import org.springframework.lang.Nullable;

import java.util.Objects;

/**
 * @author wenchao.meng
 *         <p>
 *         Aug 21, 2017
 */
public class ClusterShardHostPort {

    private String clusterName;

    private String shardName;

    private HostPort hostPort;

    @Nullable
    private String activeDc;

    public ClusterShardHostPort(String clusterName, String shardName, String activeDc, HostPort hostPort){
        this.clusterName = clusterName;
        this.shardName = shardName;
        this.hostPort = hostPort;
        this.activeDc = activeDc;
    }

    public ClusterShardHostPort(String clusterName, String shardName, HostPort hostPort){
        this(clusterName, shardName, null, hostPort);
    }

    public ClusterShardHostPort(String clusterName, String shardName){
        this(clusterName, shardName, null, null);
    }

    public ClusterShardHostPort(HostPort hostPort){
        this(null, null, null, hostPort);
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

    @Nullable
    public String getActiveDc() {
        return activeDc;
    }

    public void setActiveDc(@Nullable String activeDc) {
        this.activeDc = activeDc;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ClusterShardHostPort that = (ClusterShardHostPort) o;
        return Objects.equals(clusterName, that.clusterName) &&
                Objects.equals(shardName, that.shardName) &&
                Objects.equals(hostPort, that.hostPort) &&
                Objects.equals(activeDc, that.activeDc);
    }

    @Override
    public int hashCode() {
        return Objects.hash(clusterName, shardName, hostPort, activeDc);
    }

    @Override
    public String toString() {
        return String.format("%s,%s[%s][%s]", clusterName, shardName, activeDc, hostPort);
    }
}
