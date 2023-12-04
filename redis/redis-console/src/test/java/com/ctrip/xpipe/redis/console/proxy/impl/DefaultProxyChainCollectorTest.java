package com.ctrip.xpipe.redis.console.proxy.impl;

import com.ctrip.xpipe.api.foundation.FoundationService;
import com.ctrip.xpipe.redis.checker.model.DcClusterShardPeer;
import com.ctrip.xpipe.redis.console.AbstractConsoleTest;
import com.ctrip.xpipe.redis.console.config.ConsoleConfig;
import com.ctrip.xpipe.redis.console.model.ProxyModel;
import com.ctrip.xpipe.redis.console.proxy.ProxyChain;
import com.ctrip.xpipe.redis.console.proxy.ProxyChainAnalyzer;
import com.ctrip.xpipe.redis.console.proxy.ProxyChainCollector;
import com.ctrip.xpipe.redis.console.reporter.DefaultHttpService;
import org.apache.http.HttpException;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.client.RestOperations;

import javax.annotation.Resource;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;
import java.util.stream.IntStream;

import static com.ctrip.xpipe.spring.AbstractSpringConfigContext.SCHEDULED_EXECUTOR;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.when;


@RunWith(MockitoJUnitRunner.class)
public class DefaultProxyChainCollectorTest extends AbstractConsoleTest {

    @Mock
    private ProxyChainAnalyzer proxyChainAnalyzer;

    @Mock
    private ConsoleConfig consoleConfig;

    @Mock
    private DefaultHttpService httpService;

    @Mock
    private RestOperations restTemplate;

    @InjectMocks
    DefaultProxyChainCollector collector;

    private Map<String,String> consoles = new HashMap<String, String>() {{
        put("dc1","http://dc1");
        put("dc2","http://dc2");
    }};

    @Before
    public void setupDefaultProxyChainCollectorTest() {
        when(httpService.getRestTemplate()).thenReturn(restTemplate);
        this.collector.setHttpService(httpService);
        when(consoleConfig.getConsoleDomains()).thenReturn(consoles);
    }

    private Map<DcClusterShardPeer, DefaultProxyChain> generateProxyChains(int cnt) {
        Map<DcClusterShardPeer, DefaultProxyChain> result = new HashMap<>();
        IntStream.range(0, cnt).forEach(i -> {
            DcClusterShardPeer dcClusterShardPeer = new DcClusterShardPeer("dc1", "cluster" + i, "shard" + i, "dc2");
            List<DefaultTunnelInfo> tunnels = new ArrayList<>();
            tunnels.add(new DefaultTunnelInfo(new ProxyModel(), "tunnel" + i));
            DefaultProxyChain proxyChain = new DefaultProxyChain("dc1", "cluster" + i, "shard" + i, "dc2", tunnels);
            result.put(dcClusterShardPeer, proxyChain);
        });

        return result;
    }

    @Test
    public void remoteDcDown_noMemLeak() {
        ResponseEntity<Map<DcClusterShardPeer, DefaultProxyChain>> resp = new ResponseEntity(generateProxyChains(100), HttpStatus.OK);
        when(restTemplate.exchange(anyString(), any(), any(), any(ParameterizedTypeReference.class), anyString()))
                .thenReturn(resp);
        IntStream.range(0, 10).forEach(i -> collector.fetchAllDcProxyChains());

        Map<String, Map<DcClusterShardPeer, ProxyChain>> dcProxyChainMap = collector.getDcProxyChainMap();
        for (Map<DcClusterShardPeer, ProxyChain> proxyChainMap: dcProxyChainMap.values()) {
            for (ProxyChain proxyChain: proxyChainMap.values()) {
                Assert.assertEquals(2, proxyChain.getTunnelInfos().size());
            }
        }

        doAnswer(inov -> {
            String uri = inov.getArgument(0);
            String host = consoles.values().iterator().next();
            if (uri.startsWith(host)) throw new HttpException("mock");
            else return resp;
        }).when(restTemplate).exchange(anyString(), any(), any(), any(ParameterizedTypeReference.class));

        IntStream.range(0, 10).forEach(i -> collector.fetchAllDcProxyChains());
        dcProxyChainMap = collector.getDcProxyChainMap();
        for (Map<DcClusterShardPeer, ProxyChain> proxyChainMap: dcProxyChainMap.values()) {
            for (ProxyChain proxyChain: proxyChainMap.values()) {
                Assert.assertEquals(2, proxyChain.getTunnelInfos().size());
            }
        }
    }


}