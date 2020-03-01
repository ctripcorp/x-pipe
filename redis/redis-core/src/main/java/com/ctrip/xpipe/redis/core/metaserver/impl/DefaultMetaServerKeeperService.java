package com.ctrip.xpipe.redis.core.metaserver.impl;

import com.ctrip.xpipe.redis.core.metaserver.META_SERVER_SERVICE;
import com.ctrip.xpipe.redis.core.metaserver.MetaServerKeeperService;
import com.google.common.collect.Lists;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.MediaType;

import java.util.List;
import java.util.function.Supplier;

/**
 * @author chen.zhu
 * <p>
 * Feb 25, 2020
 */
public class DefaultMetaServerKeeperService extends AbstractMetaService implements MetaServerKeeperService {

    private Supplier<String> metaServerAddress;

    public DefaultMetaServerKeeperService(Supplier<String> metaServerAddress) {
        this.metaServerAddress = metaServerAddress;
    }

    public DefaultMetaServerKeeperService(int retryTimes, int retryIntervalMilli, Supplier<String> metaServerAddress) {
        super(retryTimes, retryIntervalMilli);
        this.metaServerAddress = metaServerAddress;
    }

    @Override
    protected List<String> getMetaServerList() {
        return Lists.newArrayList(metaServerAddress.get());
    }

    @Override
    public KeeperContainerTokenStatusResponse refreshKeeperContainerTokenStatus(KeeperContainerTokenStatusRequest request) {
        HttpHeaders httpHeaders = new HttpHeaders();
        httpHeaders.add(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON_UTF8_VALUE);
        HttpEntity<Object> entity = new HttpEntity<Object>(request, httpHeaders);
        return restTemplate.exchange(META_SERVER_SERVICE.KEEPER_TOKEN_STATUS.getRealPath(metaServerAddress.get()),
                        HttpMethod.POST,
                        entity,
                        KeeperContainerTokenStatusResponse.class).getBody();

    }
}
