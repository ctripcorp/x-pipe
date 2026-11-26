package com.ctrip.xpipe.redis.console.service.logicalbu;

import java.util.ArrayList;
import java.util.List;

/**
 * Response of POST /console/logical-bus/keeperState: per logical BU, per IDC, how many TFS keepercontainers are
 * HEALTHY / DOWN / PENDING / UNKNOWN, plus every container so the non-healthy ones can be listed.
 */
public class KeeperStateResult {

    /** Container state = worst published keeper health on it; UNKNOWN when the container could not be asked. */
    public enum ContainerState {
        HEALTHY,
        /** at least one keeper DOWN */
        DOWN,
        /** at least one keeper RECOVERED_PENDING, none DOWN (SICK counts as healthy) */
        PENDING,
        /** keepercontainer unreachable / timeout / error */
        UNKNOWN
    }

    private List<BuState> results = new ArrayList<>();

    public List<BuState> getResults() {
        return results;
    }

    public static class BuState {
        private long logicalBuId;
        private String logicalBuName;
        private List<IdcState> idcs = new ArrayList<>();
        /** set when the BU itself could not be loaded; idcs is empty then */
        private OpFailure failure;

        public BuState() {
        }

        public BuState(long logicalBuId) {
            this.logicalBuId = logicalBuId;
        }

        public IdcState idc(String name) {
            for (IdcState idc : idcs) {
                if (idc.getIdc().equals(name)) {
                    return idc;
                }
            }
            IdcState idc = new IdcState(name);
            idcs.add(idc);
            return idc;
        }

        public long getLogicalBuId() {
            return logicalBuId;
        }

        public String getLogicalBuName() {
            return logicalBuName;
        }

        public BuState setLogicalBuName(String logicalBuName) {
            this.logicalBuName = logicalBuName;
            return this;
        }

        public List<IdcState> getIdcs() {
            return idcs;
        }

        public OpFailure getFailure() {
            return failure;
        }

        public BuState setFailure(OpFailure failure) {
            this.failure = failure;
            return this;
        }
    }

    public static class IdcState {
        private String idc;
        private int healthy;
        private int down;
        private int pending;
        private int unknown;
        private List<ContainerInfo> containers = new ArrayList<>();

        public IdcState() {
        }

        public IdcState(String idc) {
            this.idc = idc;
        }

        public void add(ContainerInfo container) {
            containers.add(container);
            switch (container.getState()) {
                case HEALTHY: healthy++; break;
                case DOWN: down++; break;
                case PENDING: pending++; break;
                default: unknown++;
            }
        }

        public String getIdc() {
            return idc;
        }

        public int getHealthy() {
            return healthy;
        }

        public int getDown() {
            return down;
        }

        public int getPending() {
            return pending;
        }

        public int getUnknown() {
            return unknown;
        }

        public List<ContainerInfo> getContainers() {
            return containers;
        }
    }

    public static class ContainerInfo {
        private long id;
        /** hostname:port when the container has a hostname, ip:port otherwise */
        private String address;
        private String ip;
        private String hostname;
        private ContainerState state;
        private int keeperCount;
        private int downKeeperCount;
        private int pendingKeeperCount;
        /** UNKNOWN only: error code + detail, e.g. "KEEPER_UNREACHABLE ConnectException: Connection refused" */
        private String reason;

        public long getId() {
            return id;
        }

        public ContainerInfo setId(long id) {
            this.id = id;
            return this;
        }

        public String getAddress() {
            return address;
        }

        public ContainerInfo setAddress(String address) {
            this.address = address;
            return this;
        }

        public String getIp() {
            return ip;
        }

        public ContainerInfo setIp(String ip) {
            this.ip = ip;
            return this;
        }

        public String getHostname() {
            return hostname;
        }

        public ContainerInfo setHostname(String hostname) {
            this.hostname = hostname;
            return this;
        }

        public ContainerState getState() {
            return state;
        }

        public ContainerInfo setState(ContainerState state) {
            this.state = state;
            return this;
        }

        public int getKeeperCount() {
            return keeperCount;
        }

        public ContainerInfo setKeeperCount(int keeperCount) {
            this.keeperCount = keeperCount;
            return this;
        }

        public int getDownKeeperCount() {
            return downKeeperCount;
        }

        public ContainerInfo setDownKeeperCount(int downKeeperCount) {
            this.downKeeperCount = downKeeperCount;
            return this;
        }

        public int getPendingKeeperCount() {
            return pendingKeeperCount;
        }

        public ContainerInfo setPendingKeeperCount(int pendingKeeperCount) {
            this.pendingKeeperCount = pendingKeeperCount;
            return this;
        }

        public String getReason() {
            return reason;
        }

        public ContainerInfo setReason(String reason) {
            this.reason = reason;
            return this;
        }
    }
}
