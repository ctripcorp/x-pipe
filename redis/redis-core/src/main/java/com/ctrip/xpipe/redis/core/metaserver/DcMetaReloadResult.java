package com.ctrip.xpipe.redis.core.metaserver;

/**
 * Result of one MetaServer instance for {@code POST /api/dcmeta/reload}: the entry instance pulled DcMeta from console
 * once and every instance of the dc applied it right away (same apply path as the periodic refresh).
 */
public class DcMetaReloadResult {

    public static final String PATH_RELOAD = "/api/dcmeta/reload";

    public enum Status {
        /** New meta applied; changed shards were handed to election (priority-only changes are rate limited). */
        APPLIED,
        /** Meta pulled, nothing differed. */
        NO_CHANGE,
        /** Skipped: a clusterModified arrived after the pull started (meta.sync.delay.milli protection). */
        SKIPPED_RECENT_MODIFY,
        /** Rejected by META_MODIFY_PROTECT_COUNT (too many structural changes at once). */
        REJECTED_TOO_MANY,
        /** Pull or apply threw, or the instance could not be reached. */
        FAILED;

        public boolean isSuccess() {
            return this == APPLIED || this == NO_CHANGE;
        }
    }

    private int serverId;
    private String address;
    private Status status;
    private String message;

    public DcMetaReloadResult() {
    }

    public DcMetaReloadResult(int serverId, String address, Status status, String message) {
        this.serverId = serverId;
        this.address = address;
        this.status = status;
        this.message = message;
    }

    public int getServerId() {
        return serverId;
    }

    public DcMetaReloadResult setServerId(int serverId) {
        this.serverId = serverId;
        return this;
    }

    public String getAddress() {
        return address;
    }

    public DcMetaReloadResult setAddress(String address) {
        this.address = address;
        return this;
    }

    public Status getStatus() {
        return status;
    }

    public DcMetaReloadResult setStatus(Status status) {
        this.status = status;
        return this;
    }

    public String getMessage() {
        return message;
    }

    public DcMetaReloadResult setMessage(String message) {
        this.message = message;
        return this;
    }

    @Override
    public String toString() {
        return "server_" + serverId + "(" + address + ")=" + status + (message == null ? "" : ":" + message);
    }
}
