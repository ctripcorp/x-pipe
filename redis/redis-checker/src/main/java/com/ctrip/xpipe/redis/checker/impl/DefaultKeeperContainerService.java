package com.ctrip.xpipe.redis.checker.impl;

import com.ctrip.xpipe.redis.checker.KeeperContainerCheckerService;
import com.ctrip.xpipe.redis.core.entity.KeeperDiskInfo;
import com.ctrip.xpipe.redis.core.entity.KeeperInstanceMeta;
import com.ctrip.xpipe.redis.core.entity.KeeperTransMeta;
import com.ctrip.xpipe.redis.core.service.AbstractService;
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

    private static final String DEFAULT_KEEPER_CONTAINER_PORT = "8080";

    @Override
    public KeeperDiskInfo getKeeperDiskInfo(String keeperContainerIp) throws RestClientException {
        return restTemplate.exchange(HTTP_PREFIX + keeperContainerIp + ":" + DEFAULT_KEEPER_CONTAINER_PORT + PATH_GET_KEEPER_DISK_INFO,
                HttpMethod.GET, null, KeeperDiskInfo.class).getBody();
    }

    @Override
    public boolean setKeeperContainerDiskIOLimit(String keeperContainerIp, int keeperContainerPort, int limitInByte) {
        Boolean rst = restTemplate.postForObject("http://{ip}:{port}/keepers/limit/totalIO?limit={limit}",
                null, Boolean.class, keeperContainerIp, keeperContainerPort, limitInByte);
        return null != rst && rst;
    }

    @Override
    public List<KeeperInstanceMeta> getAllKeepers(String keeperContainerIp) {
        return restTemplate.exchange(String.format("http://%s:8080/keepers", keeperContainerIp), HttpMethod.GET, null,
                new ParameterizedTypeReference<List<KeeperInstanceMeta>>() {}).getBody();
    }

    @Override
    public void resetKeeper(String activeKeeperIp, Long replId) {
        KeeperTransMeta keeperInstanceMeta = new KeeperTransMeta();
        keeperInstanceMeta.setReplId(replId);
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        HttpEntity<KeeperTransMeta> requestEntity = new HttpEntity<>(keeperInstanceMeta, headers);
        restTemplate.exchange(String.format("http://%s:8080/keepers/election/reset", activeKeeperIp),
                HttpMethod.POST, requestEntity, Void.class);
    }

    @Override
    public void releaseRdb(String ip, int port, Long replId) {
        KeeperTransMeta keeperInstanceMeta = new KeeperTransMeta();
        keeperInstanceMeta.setReplId(replId);
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        HttpEntity<KeeperTransMeta> requestEntity = new HttpEntity<>(keeperInstanceMeta, headers);
        restTemplate.exchange(String.format("http://%s:8080/keepers/rdb/release", ip),
                HttpMethod.DELETE, requestEntity, Void.class);
    }

}
