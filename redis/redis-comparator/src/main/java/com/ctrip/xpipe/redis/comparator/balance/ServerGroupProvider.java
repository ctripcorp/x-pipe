package com.ctrip.xpipe.redis.comparator.balance;

import java.util.List;

/**
 * 本 CMS group 的 server 列表（D23 / §4.7）。
 * {@link CompareTaskAssigner} 只依赖本接口；单测注入假实现，不访问网络。
 */
public interface ServerGroupProvider {

    /**
     * 返回本 group 的 ciCode 列表（不必排序）。
     * 拉取失败必须抛异常，以便分配器区分「不可达」与「未配置」。
     */
    List<String> listCiCodes();
}
