package com.ctrip.xpipe.redis.checker.impl;

import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.redis.checker.KeeperContainerCheckerService;
import com.ctrip.xpipe.redis.core.entity.KeeperDiskInfo;
import com.ctrip.xpipe.redis.core.entity.KeeperInstanceMeta;
import com.ctrip.xpipe.redis.core.entity.KeeperTransMeta;
import com.ctrip.xpipe.redis.core.keeper.container.KeeperContainerAddressInfo;
import com.ctrip.xpipe.redis.core.keeper.container.KeeperContainerAddressService;
import com.ctrip.xpipe.redis.core.keeper.container.HostNameVerification;
import com.ctrip.xpipe.redis.core.service.AbstractService;
import com.ctrip.xpipe.utils.StringUtil;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestClientException;

import java.util.List;

@Service
public class DefaultKeeperContainerService extends AbstractService implements KeeperContainerCheckerService {

    private static final String HTTP_PREFIX = "http://";
    private static final String PATH_GET_KEEPER_DISK_INFO = "/keepers/disk";

    private KeeperContainerAddressService keeperContainerAddressService;

    public void setKeeperContainerAddressService(KeeperContainerAddressService keeperContainerAddressService) {
        this.keeperContainerAddressService = keeperContainerAddressService;
    }

    /**
     * 统一的地址解析逻辑：
     * 1. 已有真实 IP（物理机路径）直接使用
     * 2. 空串/0.0.0.0/hostname 形态走 ZK 缓存反查
     * 
     * @param ipOrHostname IP 或 hostname
     * @param port 端口
     * @return 解析后的 HostPort
     * @throws IllegalStateException hostname 无法解析时抛出
     */
    public HostPort resolveAddress(String ipOrHostname, int port) {
        // 空串直接尝试解析
        if (StringUtil.isEmpty(ipOrHostname)) {
            throw new IllegalStateException("empty address cannot be resolved");
        }
        
        // 已有真实 IP（物理机路径）直接使用
        if (isRealIpAddress(ipOrHostname)) {
            return new HostPort(ipOrHostname, port);
        }
        
        // hostname 形态走 ZK 缓存反查
        if (keeperContainerAddressService == null) {
            throw new IllegalStateException("KeeperContainerAddressService not initialized");
        }
        
        String resolvedIp = keeperContainerAddressService.getIpByHostname(ipOrHostname);
        if (resolvedIp == null) {
            throw new IllegalStateException(
                    "no alive keepercontainer for hostname: " + ipOrHostname);
        }
        
        return new HostPort(resolvedIp, port);
    }
    
    /**
     * 判断是否为真实 IP 地址（非空、非 0.0.0.0 占位符）
     */
    private boolean isRealIpAddress(String address) {
        if (StringUtil.isEmpty(address)) {
            return false;
        }
        // 0.0.0.0 是容器登记时的占位符，不是真实 IP
        if ("0.0.0.0".equals(address)) {
            return false;
        }
        // 简单判断：包含点号且不包含字母，视为 IP
        // hostname 通常包含字母
        return address.contains(".") && !address.matches(".*[a-zA-Z].*");
    }
    
    /**
     * 构建带 hostname 校验头的 HTTP headers
     */
    private HttpHeaders buildHeadersWithHostname(String hostname) {
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        if (!StringUtil.isEmpty(hostname)) {
            headers.set(HostNameVerification.HEADER_TARGET_HOST_NAME, hostname);
        }
        return headers;
    }

    @Override
    public KeeperDiskInfo getKeeperDiskInfo(String keeperContainerIp) throws RestClientException {
        HostPort address = resolveAddress(keeperContainerIp, KeeperContainerAddressInfo.KEEPER_CONTAINER_PORT);
        return restTemplate.exchange(HTTP_PREFIX + address.getHost() + ":" + address.getPort() + PATH_GET_KEEPER_DISK_INFO,
                HttpMethod.GET, null, KeeperDiskInfo.class).getBody();
    }

    @Override
    public boolean setKeeperContainerDiskIOLimit(String keeperContainerIp, int keeperContainerPort, int limitInByte) {
        HostPort address = resolveAddress(keeperContainerIp, keeperContainerPort);
        Boolean rst = restTemplate.postForObject("http://{ip}:{port}/keepers/limit/totalIO?limit={limit}",
                null, Boolean.class, address.getHost(), address.getPort(), limitInByte);
        return null != rst && rst;
    }

    @Override
    public List<KeeperInstanceMeta> getAllKeepers(String keeperContainerIp) {
        HostPort address = resolveAddress(keeperContainerIp, KeeperContainerAddressInfo.KEEPER_CONTAINER_PORT);
        return restTemplate.exchange(String.format("http://%s:%d/keepers", address.getHost(), address.getPort()), 
                HttpMethod.GET, null,
                new ParameterizedTypeReference<List<KeeperInstanceMeta>>() {}).getBody();
    }

    @Override
    public void resetKeeper(String activeKeeperIp, Long replId) {
        HostPort address = resolveAddress(activeKeeperIp, KeeperContainerAddressInfo.KEEPER_CONTAINER_PORT);
        KeeperTransMeta keeperInstanceMeta = new KeeperTransMeta();
        keeperInstanceMeta.setReplId(replId);
        
        // 对于 hostname 形态，需要携带 hostname header 用于容器端自校验
        String hostname = isRealIpAddress(activeKeeperIp) ? null : activeKeeperIp;
        HttpHeaders headers = buildHeadersWithHostname(hostname);
        
        HttpEntity<KeeperTransMeta> requestEntity = new HttpEntity<>(keeperInstanceMeta, headers);
        restTemplate.exchange(String.format("http://%s:%d/keepers/election/reset", address.getHost(), address.getPort()),
                HttpMethod.POST, requestEntity, Void.class);
    }

    @Override
    public void releaseRdb(String ip, int port, Long replId) {
        HostPort address = resolveAddress(ip, port);
        KeeperTransMeta keeperInstanceMeta = new KeeperTransMeta();
        keeperInstanceMeta.setReplId(replId);
        
        // 对于 hostname 形态，需要携带 hostname header 用于容器端自校验
        String hostname = isRealIpAddress(ip) ? null : ip;
        HttpHeaders headers = buildHeadersWithHostname(hostname);
        
        HttpEntity<KeeperTransMeta> requestEntity = new HttpEntity<>(keeperInstanceMeta, headers);
        restTemplate.exchange(String.format("http://%s:%d/keepers/rdb/release", address.getHost(), address.getPort()),
                HttpMethod.DELETE, requestEntity, Void.class);
    }

}
