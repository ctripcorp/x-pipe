package com.ctrip.xpipe.redis.core.metaserver.impl;

import com.ctrip.xpipe.redis.core.metaserver.META_SERVER_SERVICE;
import com.ctrip.xpipe.redis.core.metaserver.MetaServerConsoleService;
import com.ctrip.xpipe.redis.core.metaserver.MetaServerKeeperService;
import com.google.common.base.Function;
import com.google.common.collect.Lists;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.MediaType;

import java.util.ArrayList;
import java.util.List;

/**
 * @author chen.zhu
 * <p>
 * Feb 25, 2020
 */
public class DefaultMetaServerKeeperService extends AbstractMetaService implements MetaServerKeeperService {

    private List<String> metaServerAddress;

    private String keeperToeknStatus;

    public DefaultMetaServerKeeperService(String metaServerAddress) {
        this.metaServerAddress = Lists.newArrayList(metaServerAddress);
        this.keeperToeknStatus = META_SERVER_SERVICE.KEEPER_TOKEN_STATUS.getRealPath(metaServerAddress);
    }

    @Override
    protected List<String> getMetaServerList() {
        return metaServerAddress;
    }

    @Override
    public KeeperContainerTokenStatusResponse refreshKeeperContainerTokenStatus(KeeperContainerTokenStatusRequest request) {
        HttpHeaders httpHeaders = new HttpHeaders();
        httpHeaders.add(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON_UTF8_VALUE);
        HttpEntity<Object> entity = new HttpEntity<Object>(request, httpHeaders);
        return pollMetaServer(new Function<String, KeeperContainerTokenStatusResponse>() {
            @Override
            public KeeperContainerTokenStatusResponse apply(String metaServerAddress) {
                return restTemplate.exchange(META_SERVER_SERVICE.KEEPER_TOKEN_STATUS.getRealPath(metaServerAddress),
                        HttpMethod.POST,
                        entity,
                        KeeperContainerTokenStatusResponse.class).getBody();
            }
        });
    }
}
