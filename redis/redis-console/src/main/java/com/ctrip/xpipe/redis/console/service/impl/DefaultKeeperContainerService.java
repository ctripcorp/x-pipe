package com.ctrip.xpipe.redis.console.service.impl;

import com.ctrip.xpipe.endpoint.HostPort;
import com.ctrip.xpipe.redis.console.service.KeeperContainerCheckerService;
import com.ctrip.xpipe.redis.core.entity.KeeperDiskInfo;
import com.ctrip.xpipe.redis.core.entity.KeeperInstanceMeta;
import com.ctrip.xpipe.redis.core.entity.KeeperTransMeta;
import com.ctrip.xpipe.redis.core.keeper.container.HostNameVerification;
import com.ctrip.xpipe.redis.core.keeper.container.KeeperContainerAddressInfo;
import com.ctrip.xpipe.redis.core.keeper.container.KeeperContainerAddressService;
import com.ctrip.xpipe.redis.core.service.AbstractService;
import com.ctrip.xpipe.utils.StringUtil;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestClientException;

import java.util.List;

/**
 * Console 直连 KeeperContainer 的服务实现
 * 提供统一的地址解析逻辑，支持 IP 和 hostname 两种形态
 */
@Service
public class DefaultKeeperContainerService extends AbstractService implements KeeperContainerCheckerService {

    private static final String HTTP_PREFIX = "http://";
    private static final String PATH_GET_KEEPER_DISK_INFO = "/keepers/disk";

    @Autowired
    private KeeperContainerAddressService keeperContainerAddressService;

    /**
     * 解析地址：hostname 不为空则用它反查 IP，为空则直接使用 ip
     * 
     * @param ip IP 地址
     * @param hostname 主机名
     * @param port 端口
     * @return 解析后的 HostPort
     * @throws IllegalStateException hostname 无法解析时抛出
     */
    private HostPort resolveAddress(String ip, String hostname, int port) {
        String address = ip;
        
        // hostname 不为空则使用 hostname 反查 IP
        if (!StringUtil.isEmpty(hostname)) {
            if (keeperContainerAddressService == null) {
                throw new IllegalStateException("KeeperContainerAddressService not initialized");
            }
            
            String resolvedIp = keeperContainerAddressService.getIpByHostname(hostname);
            if (resolvedIp == null) {
                throw new IllegalStateException("no alive keepercontainer for hostname: " + hostname);
            }
            address = resolvedIp;
        }
        
        return new HostPort(address, port);
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
        HostPort address = new HostPort(keeperContainerIp, KeeperContainerAddressInfo.KEEPER_CONTAINER_PORT);
        return restTemplate.exchange(HTTP_PREFIX + address.getHost() + ":" + address.getPort() + PATH_GET_KEEPER_DISK_INFO,
                HttpMethod.GET, null, KeeperDiskInfo.class).getBody();
    }

    @Override
    public boolean setKeeperContainerDiskIOLimit(String keeperContainerIp, int keeperContainerPort, int limitInByte) {
        HostPort address = new HostPort(keeperContainerIp, keeperContainerPort);
        Boolean rst = restTemplate.postForObject("http://{ip}:{port}/keepers/limit/totalIO?limit={limit}",
                null, Boolean.class, address.getHost(), address.getPort(), limitInByte);
        return null != rst && rst;
    }

    @Override
    public List<KeeperInstanceMeta> getAllKeepers(String keeperContainerIp) {
        HostPort address = new HostPort(keeperContainerIp, KeeperContainerAddressInfo.KEEPER_CONTAINER_PORT);
        return restTemplate.exchange(String.format("http://%s:%d/keepers", address.getHost(), address.getPort()), 
                HttpMethod.GET, null,
                new ParameterizedTypeReference<List<KeeperInstanceMeta>>() {}).getBody();
    }

    @Override
    public void resetKeeper(String ip, String hostname, Long replId) {
        HostPort address = resolveAddress(ip, hostname, KeeperContainerAddressInfo.KEEPER_CONTAINER_PORT);
        
        KeeperTransMeta keeperInstanceMeta = new KeeperTransMeta();
        keeperInstanceMeta.setReplId(replId);
        
        // 如果使用 hostname,需要携带 hostname header 用于容器端自校验
        HttpHeaders headers = buildHeadersWithHostname(hostname);
        
        HttpEntity<KeeperTransMeta> requestEntity = new HttpEntity<>(keeperInstanceMeta, headers);
        restTemplate.exchange(String.format("http://%s:%d/keepers/election/reset", address.getHost(), address.getPort()),
                HttpMethod.POST, requestEntity, Void.class);
    }

    @Override
    public void releaseRdb(String ip, String hostname, int port, Long replId) {
        HostPort address = resolveAddress(ip, hostname, port);
        
        KeeperTransMeta keeperInstanceMeta = new KeeperTransMeta();
        keeperInstanceMeta.setReplId(replId);
        
        // 如果使用 hostname,需要携带 hostname header 用于容器端自校验
        HttpHeaders headers = buildHeadersWithHostname(hostname);
        
        HttpEntity<KeeperTransMeta> requestEntity = new HttpEntity<>(keeperInstanceMeta, headers);
        restTemplate.exchange(String.format("http://%s:%d/keepers/rdb/release", address.getHost(), address.getPort()),
                HttpMethod.DELETE, requestEntity, Void.class);
    }

}
