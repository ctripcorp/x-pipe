package com.ctrip.xpipe.redis.checker.resource;

import com.ctrip.xpipe.redis.checker.CheckerConsoleService;
import com.ctrip.xpipe.redis.checker.model.CheckerStatus;
import com.ctrip.xpipe.redis.checker.model.HealthCheckResult;
import com.ctrip.xpipe.redis.checker.model.ProxyTunnelInfo;
import com.ctrip.xpipe.redis.core.console.ConsoleCheckerPath;
import com.ctrip.xpipe.redis.core.entity.XpipeMeta;
import com.ctrip.xpipe.redis.core.service.AbstractService;
import org.springframework.context.annotation.Lazy;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.http.HttpMethod;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Component;

import java.util.List;

/**
 * @author lishanglin
 * date 2021/3/16
 */
@Lazy
@Component
public class DefaultCheckerConsoleService extends AbstractService implements CheckerConsoleService {

    private static final ParameterizedTypeReference<List<ProxyTunnelInfo>> proxyTunnelInfosTypeDef = new ParameterizedTypeReference<List<ProxyTunnelInfo>>(){};

    public XpipeMeta getXpipeMeta(String console, int clusterPartIndex) {
        return restTemplate.getForObject(console + ConsoleCheckerPath.PATH_GET_META,
                XpipeMeta.class, clusterPartIndex);
    }

    public List<ProxyTunnelInfo> getProxyTunnelInfos(String console) {
        ResponseEntity<List<ProxyTunnelInfo>> resp = restTemplate.exchange(console + ConsoleCheckerPath.PATH_GET_PROXY_CHAINS,
                HttpMethod.GET, null, proxyTunnelInfosTypeDef);
        return resp.getBody();
    }

    @Override
    public void ack(CheckerStatus checkerStatus) {

    }

    @Override
    public void report(HealthCheckResult result) {

    }
}
