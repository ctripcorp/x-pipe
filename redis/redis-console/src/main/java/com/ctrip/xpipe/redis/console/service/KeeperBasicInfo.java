package com.ctrip.xpipe.redis.console.service;

import com.ctrip.xpipe.utils.StringUtil;

/**
 * @author wenchao.meng
 *         <p>
 *         Jul 17, 2017
 */
public class KeeperBasicInfo {

    private long keeperContainerId;
    private String host;
    private int port;

    /**
     * 目标 keepercontainer 的 pod hostname；分配到物理机容器时为空串。
     * <p>
     * 容器化后分配那一刻拿不到 IP（pod 可能尚未调度），host 会是容器登记时的占位值，
     * 真正的身份在这里。落库后由 redis_tbl.host_name 承载。
     */
    private String hostName;

    public KeeperBasicInfo() {
    }

    public KeeperBasicInfo(long keeperContainerId, String host, int port) {
        this(keeperContainerId, host, port, null);
    }

    public KeeperBasicInfo(long keeperContainerId, String host, int port, String hostName) {
        this.keeperContainerId = keeperContainerId;
        this.host = host;
        this.port = port;
        this.hostName = hostName;
    }

    public long getKeeperContainerId() {
        return keeperContainerId;
    }

    public void setKeeperContainerId(long keeperContainerId) {
        this.keeperContainerId = keeperContainerId;
    }

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public String getHostName() {
        return hostName;
    }

    public void setHostName(String hostName) {
        this.hostName = hostName;
    }

    /**
     * 是否分配到了容器（而非物理机）。容器与物理机在端口记账、校验、寻址上走两套逻辑，
     * 判断依据统一为 hostName 是否有值。
     */
    public boolean isPodKeeper() {
        return !StringUtil.isEmpty(hostName);
    }

    @Override
    public String toString() {
        if (isPodKeeper()) {
            return String.format("[%s:%d, keeperContainerId:%d]", hostName, port, keeperContainerId);
        }
        return String.format("[%s:%d, keeperContainerId:%d]", host, port, keeperContainerId);
    }
}
