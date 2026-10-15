package com.ctrip.xpipe.cluster;

import org.apache.curator.framework.recipes.leader.LeaderLatch;

/**
 * {@link RebuildableLeaderLatch} 最近一次断链探测的结果快照（keeper-dynamic-ip spec §5.6）。
 * <p>
 * 只由探测线程构造，经 volatile 字段发布后不再修改，供运维接口（Ih）只读展示。
 * 可选字段在对应检查没做到时为 null：连接不健康或 ourPath 为 null 时不查 ZK，
 * 就没有 pathExists / ephemeralOwner / sessionId。
 */
public class LeaderLatchProbeStatus {

    public enum Verdict {
        /** 还没有探测结果：刚 start，或刚重建完。 */
        NOT_PROBED,
        HEALTHY,
        UNHEALTHY,
        /** 前提不成立（未 STARTED / 连接不健康 / ZK 调用本身失败），不下结论，也不计入连续不健康时长。 */
        INCONCLUSIVE
    }

    public enum Reason {
        NOT_PROBED(Verdict.NOT_PROBED),
        OK(Verdict.HEALTHY),
        NOT_STARTED(Verdict.INCONCLUSIVE),
        DISCONNECTED(Verdict.INCONCLUSIVE),
        PROBE_ERROR(Verdict.INCONCLUSIVE),
        /** 判据①：ourPath 为 null。 */
        OUR_PATH_NULL(Verdict.UNHEALTHY),
        /** 判据②：ourPath 在 ZK 上查不到。 */
        PATH_NOT_EXIST(Verdict.UNHEALTHY),
        /** 判据③：ourPath 的 ephemeralOwner 不是当前 session。 */
        OWNER_MISMATCH(Verdict.UNHEALTHY);

        private final Verdict verdict;

        Reason(Verdict verdict) {
            this.verdict = verdict;
        }

        public Verdict verdict() {
            return verdict;
        }
    }

    private final String latchPath;
    private final String id;
    private final Reason reason;
    private final LeaderLatch.State state;
    private final String ourPath;
    private final long probeTimeMillis;

    private Boolean pathExists;
    private Long ephemeralOwner;
    private Long sessionId;
    private Integer resultCode;
    private long unhealthyDurationMillis;

    LeaderLatchProbeStatus(String latchPath, String id, Reason reason, LeaderLatch.State state,
                           String ourPath, long probeTimeMillis) {
        this.latchPath = latchPath;
        this.id = id;
        this.reason = reason;
        this.state = state;
        this.ourPath = ourPath;
        this.probeTimeMillis = probeTimeMillis;
    }

    LeaderLatchProbeStatus withNode(boolean pathExists, Long ephemeralOwner, Long sessionId) {
        this.pathExists = pathExists;
        this.ephemeralOwner = ephemeralOwner;
        this.sessionId = sessionId;
        return this;
    }

    LeaderLatchProbeStatus withResultCode(int resultCode) {
        this.resultCode = resultCode;
        return this;
    }

    LeaderLatchProbeStatus withUnhealthyDuration(long unhealthyDurationMillis) {
        this.unhealthyDurationMillis = unhealthyDurationMillis;
        return this;
    }

    public boolean isHealthy() {
        return Verdict.HEALTHY == getVerdict();
    }

    public boolean isUnhealthy() {
        return Verdict.UNHEALTHY == getVerdict();
    }

    public Verdict getVerdict() {
        return reason.verdict();
    }

    public Reason getReason() {
        return reason;
    }

    public String getLatchPath() {
        return latchPath;
    }

    public String getId() {
        return id;
    }

    public LeaderLatch.State getState() {
        return state;
    }

    public String getOurPath() {
        return ourPath;
    }

    public Boolean getPathExists() {
        return pathExists;
    }

    public Long getEphemeralOwner() {
        return ephemeralOwner;
    }

    public Long getSessionId() {
        return sessionId;
    }

    /** ZK 返回码，仅 {@link Reason#PROBE_ERROR} 时有值。 */
    public Integer getResultCode() {
        return resultCode;
    }

    public long getProbeTimeMillis() {
        return probeTimeMillis;
    }

    /** 截至本次探测的连续不健康时长；非 UNHEALTHY 时为 0。 */
    public long getUnhealthyDurationMillis() {
        return unhealthyDurationMillis;
    }

    @Override
    public String toString() {
        return "LeaderLatchProbeStatus{" +
                "latchPath='" + latchPath + '\'' +
                ", verdict=" + getVerdict() +
                ", reason=" + reason +
                ", state=" + state +
                ", ourPath='" + ourPath + '\'' +
                ", pathExists=" + pathExists +
                ", ephemeralOwner=" + (null == ephemeralOwner ? null : Long.toHexString(ephemeralOwner)) +
                ", sessionId=" + (null == sessionId ? null : Long.toHexString(sessionId)) +
                ", resultCode=" + resultCode +
                ", unhealthyDurationMillis=" + unhealthyDurationMillis +
                ", probeTimeMillis=" + probeTimeMillis +
                '}';
    }
}
