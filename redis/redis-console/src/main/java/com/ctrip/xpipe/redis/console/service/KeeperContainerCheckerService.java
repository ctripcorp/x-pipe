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

    boolean setKeeperContainerDiskIOLimit(String keeperContainerIp, int keeperContainerPort, int limitInByte);

    List<KeeperInstanceMeta> getAllKeepers(String keeperContainerIp);

    void resetKeeper(String ip, String hostname, Long replId);

    void releaseRdb(String ip, String hostname, int port, Long replId);

}
