package com.ctrip.xpipe.redis.core.keeper.container;

/**
 * 直连 keepercontainer 的命令携带目标 hostname，由容器自校验是否打对了目标。
 * <p>
 * 防的是这个场景：pod 重建后 IP 被 Cilium 回收并分配给另一个容器，而调用方缓存尚未刷新，
 * 于是命令按旧 IP 打到了新租户身上。removeKeeper、IO 限流、resetKeeper 都是写操作，
 * 打错即事故。
 * <p>
 * 契约常量放在 redis-core：校验方在 redis-keeper，发送方在 redis-console 与 redis-meta，
 * 三方必须引用同一份定义。
 */
public interface HostNameVerification {

    /**
     * 目标 hostname 的请求头。缺失时视为不参与校验，直接放行，
     * 以兼容物理机时代不带该头的存量调用方。
     */
    String HEADER_TARGET_HOST_NAME = "X-Keeper-Container-Host";

    /**
     * 421 Misdirected Request：请求被发到了无法处理它的服务器上，语义正好对应"打错目标"。
     * 不用 400，是为了让调用方能把"打错机器、应刷新地址缓存后重试"与"请求参数不合法"区分开。
     */
    int SC_MISDIRECTED_REQUEST = 421;
}
