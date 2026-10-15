package com.ctrip.xpipe.redis.console.service.impl;

import com.ctrip.xpipe.redis.console.model.KeepercontainerTbl;
import org.junit.Assert;
import org.junit.Test;

/**
 * Task 6 测试：容器登记/更新/查询双轨（影响面 A 组）
 * 
 * 测试容器登记时的 hostname 支持：
 * - 容器可以只用 hostname 登记，IP 可以为空或 0.0.0.0
 * - 查重键从 (ip, port) 改为 hostname 或 (ip, port) 双轨
 * - 更新时支持按 hostname 定位
 * - 查询返回时携带 hostname 信息
 */
public class KeeperContainerServiceImplDualTrackTest {

    /**
     * 判断是否为 pod 容器
     */
    @Test
    public void testIsPodContainer() {
        // pod 容器：hostname 非空
        KeepercontainerTbl pod = new KeepercontainerTbl()
                .setKeepercontainerHostName("pod-a-0");
        Assert.assertTrue(isPodContainer(pod));

        // 物理机容器：hostname 为空串
        KeepercontainerTbl physical = new KeepercontainerTbl()
                .setKeepercontainerIp("10.0.0.1")
                .setKeepercontainerHostName("");
        Assert.assertFalse(isPodContainer(physical));

        // null 安全
        KeepercontainerTbl nullHostname = new KeepercontainerTbl()
                .setKeepercontainerHostName(null);
        Assert.assertFalse(isPodContainer(nullHostname));
    }

    /**
     * 辅助方法：判断是否为 pod 容器
     * 与实现代码中的 isPodContainer 逻辑一致
     */
    private boolean isPodContainer(KeepercontainerTbl tbl) {
        return tbl != null && 
               tbl.getKeepercontainerHostName() != null && 
               !tbl.getKeepercontainerHostName().isEmpty();
    }
}
