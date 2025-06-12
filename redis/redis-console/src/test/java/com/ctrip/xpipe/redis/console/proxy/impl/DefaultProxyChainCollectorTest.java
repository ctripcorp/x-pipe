package com.ctrip.xpipe.redis.console.proxy.impl;

import com.ctrip.xpipe.redis.checker.model.DcClusterShardPeer;
import com.ctrip.xpipe.redis.console.AbstractConsoleTest;
import com.ctrip.xpipe.redis.console.config.ConsoleConfig;
import com.ctrip.xpipe.redis.console.model.ProxyModel;
import com.ctrip.xpipe.redis.console.proxy.ProxyChain;
import com.ctrip.xpipe.redis.console.proxy.ProxyChainAnalyzer;
import com.ctrip.xpipe.redis.console.reporter.DefaultHttpService;
import org.apache.http.HttpException;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.client.RestOperations;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
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

    DefaultProxyChainCollector collector;

    private Map<String,String> consoles = new HashMap<String, String>() {{
        put("dc1","http://dc1");
        put("dc2","http://dc2");
    }};

    @Before
    public void setupDefaultProxyChainCollectorTest() {
        this.collector = new DefaultProxyChainCollector(proxyChainAnalyzer, consoleConfig);
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
        collector.setTaskTrigger(true);
        when(restTemplate.exchange(anyString(), any(), any(), any(ParameterizedTypeReference.class), anyString()))
                .thenReturn(resp);
        IntStream.range(0, 10).forEach(i -> collector.fetchAllDcProxyChains());

        List<ProxyChain> proxyChains = collector.getProxyChains();
        for (ProxyChain proxyChain: proxyChains) {
            Assert.assertEquals(2, proxyChain.getTunnelInfos().size());
        }

        doAnswer(inov -> {
            String uri = inov.getArgument(0);
            String host = consoles.values().iterator().next();
            if (uri.startsWith(host)) throw new HttpException("mock");
            else return resp;
        }).when(restTemplate).exchange(anyString(), any(), any(), any(ParameterizedTypeReference.class), anyString());

        IntStream.range(0, 10).forEach(i -> collector.fetchAllDcProxyChains());
        proxyChains = collector.getProxyChains();
        for (ProxyChain proxyChain: proxyChains) {
            Assert.assertEquals(2, proxyChain.getTunnelInfos().size());
        }
    }


}