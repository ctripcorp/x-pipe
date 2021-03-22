package com.ctrip.xpipe.redis.checker.resource;

import com.ctrip.xpipe.redis.checker.CheckerConsoleService;
import com.ctrip.xpipe.redis.checker.model.CheckerStatus;
import com.ctrip.xpipe.redis.checker.model.HealthCheckResult;
import com.ctrip.xpipe.redis.checker.model.ProxyTunnelInfo;
import com.ctrip.xpipe.redis.core.console.ConsoleCheckerPath;
import com.ctrip.xpipe.redis.core.entity.XpipeMeta;
import com.ctrip.xpipe.redis.core.service.AbstractService;
import com.ctrip.xpipe.redis.core.transform.DefaultSaxParser;
import com.ctrip.xpipe.utils.StringUtil;
import com.ctrip.xpipe.utils.VisibleForTesting;
import org.springframework.context.annotation.Lazy;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.http.HttpMethod;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestOperations;
import org.springframework.web.util.UriComponents;
import org.springframework.web.util.UriComponentsBuilder;
import org.xml.sax.SAXException;

import java.io.IOException;
import java.util.List;

/**
 * @author lishanglin
 * date 2021/3/16
 */
@Lazy
@Component
public class DefaultCheckerConsoleService extends AbstractService implements CheckerConsoleService {

    private static final ParameterizedTypeReference<List<ProxyTunnelInfo>> proxyTunnelInfosTypeDef = new ParameterizedTypeReference<List<ProxyTunnelInfo>>(){};

    public XpipeMeta getXpipeMeta(String console, int clusterPartIndex) throws SAXException, IOException {
        UriComponents comp = UriComponentsBuilder.fromHttpUrl(console + ConsoleCheckerPath.PATH_GET_META)
                .queryParam("format", "xml")
                .buildAndExpand(clusterPartIndex);

        String raw = restTemplate.getForObject(comp.toString(), String.class);
        if (StringUtil.isEmpty(raw)) return null;
        return DefaultSaxParser.parse(raw);
    }

    public List<ProxyTunnelInfo> getProxyTunnelInfos(String console) {
        ResponseEntity<List<ProxyTunnelInfo>> resp = restTemplate.exchange(console + ConsoleCheckerPath.PATH_GET_PROXY_CHAINS,
                HttpMethod.GET, null, proxyTunnelInfosTypeDef);
        return resp.getBody();
    }

    @Override
    public void ack(String console, CheckerStatus checkerStatus) {
        restTemplate.put(console + ConsoleCheckerPath.PATH_PUT_CHECKER_STATUS, checkerStatus);
    }

    @Override
    public void report(String console, HealthCheckResult result) {
        restTemplate.put(console + ConsoleCheckerPath.PATH_PUT_HEALTH_CHECK_RESULT, result);
    }

    @VisibleForTesting
    protected void setRestOperations(RestOperations restOperations) {
        this.restTemplate = restOperations;
    }

}
