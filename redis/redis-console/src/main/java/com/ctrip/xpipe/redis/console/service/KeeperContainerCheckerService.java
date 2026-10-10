package com.ctrip.xpipe.redis.console.service;

import com.ctrip.xpipe.redis.core.entity.KeeperDiskInfo;
import com.ctrip.xpipe.redis.core.entity.KeeperInstanceMeta;
import org.springframework.web.client.RestClientException;

import java.util.List;

/**
 * Console 直连 KeeperContainer 的服务接口
 * 提供 REST 操作：磁盘信息、IO限流、keeper实例管理、election重置、rdb释放
 */
public interface KeeperContainerCheckerService {

    KeeperDiskInfo getKeeperDiskInfo(String keeperContainerIp) throws RestClientException;

    /**
     * 设置 KeeperContainer IO 限流。hostname 非空时按机房解析实时 IP，并携带目标 hostname 校验头。
     */
    boolean setKeeperContainerDiskIOLimit(String dcName, String keeperContainerIp, String hostname,
                                          int keeperContainerPort, int limitInByte);

    List<KeeperInstanceMeta> getAllKeepers(String keeperContainerIp);

    /**
     * @param dcName keeper 所在机房，hostname 非空时用于定向反查 IP；为空则退化为查询所有机房
     */
    void resetKeeper(String dcName, String ip, String hostname, Long replId);

    void releaseRdb(String dcName, String ip, String hostname, int port, Long replId);

}
