package com.ctrip.xpipe.redis.core.keeper.container;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * 一个机房 keepercontainer 地址映射的导出快照，供远端机房 console 批量拉取后本地查询（spec §8.3）。
 * <p>
 * 三部分出自导出方同一份内部快照，互相一致：
 * <ul>
 *     <li>{@code liveHostnameToIp}：hostname → 首个存活 pod 的 IP（D-I1）</li>
 *     <li>{@code tombstones}：失去存活映射的 hostname → last-known IP 与剩余保留毫秒，只含导出时未到期的项，
 *     与 liveHostnameToIp 的 key 不相交</li>
 *     <li>{@code liveIpToHostname}：全部存活 latch 子节点 IP → hostname，同 IP 多 hostname 取字典序首，不含 tombstone</li>
 * </ul>
 * tombstone 用剩余毫秒而非到期时刻：导出方的到期时刻基于 {@code System.nanoTime()}，跨 JVM 没有意义；
 * 也不用墙钟，避免依赖两端时钟一致。接收方按「收到时刻 + 剩余毫秒」在本地到期，误差为一次请求的网络耗时。
 * <p>
 * 可变 Java Bean，便于 Jackson 序列化；导出方每次新建实例，不与内部状态共享 map。
 */
public class KeeperContainerAddressSnapshot {

    private Map<String, String> liveHostnameToIp = new HashMap<>();

    private Map<String, TombstoneEntry> tombstones = new HashMap<>();

    private Map<String, String> liveIpToHostname = new HashMap<>();

    public KeeperContainerAddressSnapshot() {
    }

    public KeeperContainerAddressSnapshot(Map<String, String> liveHostnameToIp,
                                          Map<String, TombstoneEntry> tombstones,
                                          Map<String, String> liveIpToHostname) {
        setLiveHostnameToIp(liveHostnameToIp);
        setTombstones(tombstones);
        setLiveIpToHostname(liveIpToHostname);
    }

    public Map<String, String> getLiveHostnameToIp() {
        return liveHostnameToIp;
    }

    /** 反序列化遇到 null 时置为空 map，接收方无需判空。 */
    public void setLiveHostnameToIp(Map<String, String> liveHostnameToIp) {
        this.liveHostnameToIp = null == liveHostnameToIp ? new HashMap<>() : liveHostnameToIp;
    }

    public Map<String, TombstoneEntry> getTombstones() {
        return tombstones;
    }

    public void setTombstones(Map<String, TombstoneEntry> tombstones) {
        this.tombstones = null == tombstones ? new HashMap<>() : tombstones;
    }

    public Map<String, String> getLiveIpToHostname() {
        return liveIpToHostname;
    }

    public void setLiveIpToHostname(Map<String, String> liveIpToHostname) {
        this.liveIpToHostname = null == liveIpToHostname ? new HashMap<>() : liveIpToHostname;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof KeeperContainerAddressSnapshot)) return false;
        KeeperContainerAddressSnapshot that = (KeeperContainerAddressSnapshot) o;
        return liveHostnameToIp.equals(that.liveHostnameToIp)
                && tombstones.equals(that.tombstones)
                && liveIpToHostname.equals(that.liveIpToHostname);
    }

    @Override
    public int hashCode() {
        return Objects.hash(liveHostnameToIp, tombstones, liveIpToHostname);
    }

    @Override
    public String toString() {
        return "KeeperContainerAddressSnapshot{live=" + liveHostnameToIp
                + ", tombstones=" + tombstones
                + ", liveIpToHostname=" + liveIpToHostname + '}';
    }

    /**
     * 导出时未到期的 tombstone。remainingMillis 恒为正：不足 1ms 的项导出方直接丢弃，接收方收到即视为已到期。
     */
    public static class TombstoneEntry {

        private String ip;

        private long remainingMillis;

        public TombstoneEntry() {
        }

        public TombstoneEntry(String ip, long remainingMillis) {
            this.ip = ip;
            this.remainingMillis = remainingMillis;
        }

        public String getIp() {
            return ip;
        }

        public void setIp(String ip) {
            this.ip = ip;
        }

        public long getRemainingMillis() {
            return remainingMillis;
        }

        public void setRemainingMillis(long remainingMillis) {
            this.remainingMillis = remainingMillis;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof TombstoneEntry)) return false;
            TombstoneEntry that = (TombstoneEntry) o;
            return remainingMillis == that.remainingMillis && Objects.equals(ip, that.ip);
        }

        @Override
        public int hashCode() {
            return Objects.hash(ip, remainingMillis);
        }

        @Override
        public String toString() {
            return ip + "(" + remainingMillis + "ms)";
        }
    }
}
