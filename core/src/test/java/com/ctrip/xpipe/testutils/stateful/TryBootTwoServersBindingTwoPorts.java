package com.ctrip.xpipe.testutils.stateful;

import com.ctrip.xpipe.testutils.SpringApplicationStarter;
import org.junit.Test;
import org.springframework.core.env.ConfigurableEnvironment;
import org.springframework.core.env.MapPropertySource;
import org.springframework.core.env.MutablePropertySources;
import org.springframework.web.context.support.StandardServletEnvironment;

import java.util.HashMap;
import java.util.Map;

/**
 * @author Slight
 * <p>
 * Sep 17, 2021 9:40 PM
 */
public class TryBootTwoServersBindingTwoPorts {

    @Test
    public void bootWithDifferentConfigs() throws Exception {
        System.setProperty("server.state", "1");
        startPersonServer(7000);
        System.setProperty("server.state", "2");
        startPersonServer(7001);

        blocking();
    }

    public void startPersonServer(int port) throws Exception {
        new SpringApplicationStarter(ConfigServer.class, port) {
            @Override
            protected ConfigurableEnvironment createEnvironment() {
                return new StandardServletEnvironment() {
                    @Override
                    protected void customizePropertySources(MutablePropertySources propertySources) {
                        super.customizePropertySources(propertySources);

                        Map<String, Object> properties = new HashMap<>();
                        properties.put("server.port", String.valueOf(port));
                        properties.put("server.tomcat.max-threads", String.valueOf(maxThreads));
                        propertySources.addFirst(new MapPropertySource("TestAppServerProperty", properties));
                    }
                };
            }
        }.start();
    }

    public void blocking() throws InterruptedException {
        Thread.currentThread().join();
    }

}
