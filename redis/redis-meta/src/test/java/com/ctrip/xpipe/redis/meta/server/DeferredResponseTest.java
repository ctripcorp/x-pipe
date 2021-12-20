package com.ctrip.xpipe.redis.meta.server;

import com.ctrip.xpipe.redis.core.entity.KeeperMeta;
import com.ctrip.xpipe.redis.meta.server.cluster.ClusterServers;
import com.ctrip.xpipe.redis.meta.server.cluster.SlotManager;
import com.ctrip.xpipe.redis.meta.server.cluster.impl.DefaultSlotManager;
import com.ctrip.xpipe.redis.meta.server.impl.DefaultMetaServers;
import com.ctrip.xpipe.redis.meta.server.impl.RemoteMetaServer;
import com.ctrip.xpipe.redis.meta.server.meta.DcMetaCache;
import com.ctrip.xpipe.redis.meta.server.meta.impl.DefaultDcMetaCache;
import com.ctrip.xpipe.spring.AbstractProfile;
import com.ctrip.xpipe.spring.RestTemplateFactory;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.springframework.beans.BeansException;
import org.springframework.beans.PropertyValues;
import org.springframework.beans.factory.config.InstantiationAwareBeanPostProcessor;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Profile;
import org.springframework.http.HttpMethod;
import org.springframework.http.ResponseEntity;
import org.springframework.web.client.RestOperations;

import java.beans.PropertyDescriptor;

import static com.ctrip.xpipe.AbstractTest.randomPort;

@SpringBootApplication
@Profile(AbstractProfile.PROFILE_NAME_TEST)
public class DeferredResponseTest implements InstantiationAwareBeanPostProcessor {

    private RestOperations restOperations;

    private static final int PORT = randomPort();

    private static final String PATH = "/api/meta/getactivekeeper/testcluster/shard1";

    private static boolean onTest = false;

    private ConfigurableApplicationContext applicationContext;

    private int defaultThreads;

    @Before
    public void setupDeferredResponseTest() {
        System.setProperty("server.port", PORT + "");
        defaultThreads = Integer.parseInt(System.getProperty("server.tomcat.max-threads", "200"));
        System.setProperty("server.tomcat.max-threads", "1");
        onTest = true;
        applicationContext = new SpringApplicationBuilder(DeferredResponseTest.class).run();
    }

    @After
    public void afterDeferredResponseTest() {
        onTest = false;
        System.setProperty("server.tomcat.max-threads", "" + defaultThreads);
        if (applicationContext != null) {
            applicationContext.stop();
        }
    }

    @Test
    public void test() {
        requestForActiveKeeper();
    }

    /* internal */
    private DcMetaCache mockDcMetaCache() {
        DcMetaCache dcMetaCache = Mockito.mock(DcMetaCache.class);
        Mockito.when(dcMetaCache.clusterId2DbId("testcluster")).thenReturn(1L);
        return dcMetaCache;
    }

    private SlotManager mockSlotManager() {
        SlotManager slotManager = Mockito.mock(SlotManager.class);
        Mockito.when(slotManager.getServerIdByKey(Mockito.any())).thenReturn(0);
        return slotManager;
    }

    private ClusterServers<MetaServer> mockClusterServers() {
        ClusterServers<MetaServer> clusterServers = Mockito.mock(ClusterServers.class);
        MetaServer metaServer = mockMetaServer();
        Mockito.when(clusterServers.getClusterServer(Mockito.anyByte())).thenReturn(metaServer);
        return clusterServers;
    }

    private MetaServer mockMetaServer() {
        MetaServer metaServer = Mockito.mock(RemoteMetaServer.class);
        Mockito.when(metaServer.getActiveKeeper(Mockito.anyString(), Mockito.anyString(), Mockito.any()))
                .thenAnswer(new Answer<KeeperMeta>() {
                    int circularCallCnt = 0;

                    public KeeperMeta answer(InvocationOnMock var1) throws Throwable {
                        if (circularCallCnt >= 1) {
                            return new KeeperMeta();
                        }

                        circularCallCnt++;
                        return requestForActiveKeeper();
                    }
                });
        return metaServer;
    }

    private KeeperMeta requestForActiveKeeper() {
        RestOperations restOperations = getRestOperations();
        ResponseEntity<KeeperMeta> response = restOperations.exchange("http://127.0.0.1:" + PORT + PATH,
                HttpMethod.GET, null, KeeperMeta.class);
        return response.getBody();
    }

    private RestOperations getRestOperations() {
        if (null == restOperations) {
            restOperations = RestTemplateFactory.createCommonsHttpRestTemplate();
        }

        return restOperations;
    }


    /* InstantiationAwareBeanPostProcessor */
    public Object postProcessBeforeInstantiation(Class<?> beanClass, String beanName) throws BeansException {
        // only mock Bean on DeferredResponseTest
        if (!onTest) return null;

        if (beanClass.equals(DefaultDcMetaCache.class)) {
            return mockDcMetaCache();
        }

        if (beanClass.equals(DefaultSlotManager.class)) {
            return mockSlotManager();
        }

        if (beanClass.equals(DefaultMetaServers.class)) {
            return mockClusterServers();
        }

        return null;
    }

    public boolean postProcessAfterInstantiation(Object bean, String beanName) throws BeansException {
        return true;
    }

    public PropertyValues postProcessPropertyValues(PropertyValues pvs, PropertyDescriptor[] pds, Object bean, String beanName) throws BeansException {
        return pvs;
    }

    public Object postProcessBeforeInitialization(Object bean, String beanName) throws BeansException {
        return bean;
    }

    public Object postProcessAfterInitialization(Object bean, String beanName) throws BeansException {
        return bean;
    }

}
