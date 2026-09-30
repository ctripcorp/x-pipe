package com.ctrip.xpipe.redis.core.keeper.container;

import com.ctrip.xpipe.lifecycle.AbstractLifecycle;

import java.util.HashMap;
import java.util.Map;

/**
 * 测试用的 hostname → IP 解析：不连 ZK，映射表由用例自己塞。
 * <p>
 * 生产上这个 bean 由各进程的 production profile 装配（console 的 Production /
 * CheckerContextConfig、metaserver 的 Production），都依赖 ZkClient；
 * 测试上下文没有 ZK，用这个内存实现顶上。
 * <p>
 * 默认返回空映射，等价于"当前机房没有容器形态的 keepercontainer"，
 * 物理机场景下消费方本来就不会走到 hostname 解析。
 */
public class TestCurrentDcKeeperContainerAddressService extends AbstractLifecycle
        implements CurrentDcKeeperContainerAddressService {

    private final Map<String, String> hostnameToIp = new HashMap<>();

    public void put(String hostname, String ip) {
        hostnameToIp.put(hostname, ip);
    }

    public void clear() {
        hostnameToIp.clear();
    }

    @Override
    public String getIpByHostname(String hostname) {
        if (null == hostname || hostname.isEmpty()) return null;
        return hostnameToIp.get(hostname);
    }

    @Override
    public Map<String, String> getAllHostnameToIp() {
        return new HashMap<>(hostnameToIp);
    }

    /** 不模拟 tombstone：塞进来的映射都视为存活。同 IP 多 hostname 与生产实现一致，取字典序最小者。 */
    @Override
    public String getHostnameByIp(String ip) {
        if (null == ip || ip.isEmpty()) return null;
        return hostnameToIp.entrySet().stream()
                .filter(entry -> ip.equals(entry.getValue()))
                .map(Map.Entry::getKey)
                .min(String::compareTo)
                .orElse(null);
    }
}
