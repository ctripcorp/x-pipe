package com.ctrip.xpipe.testutils;

import com.ctrip.xpipe.lifecycle.AbstractStartStoppable;
import org.springframework.boot.Banner;
import org.springframework.boot.SpringApplication;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.core.env.ConfigurableEnvironment;
import org.springframework.core.env.MapPropertySource;
import org.springframework.core.env.MutablePropertySources;
import org.springframework.web.context.support.StandardServletEnvironment;

import java.util.HashMap;
import java.util.Map;

/**
 * @author wenchao.meng
 * <p>
 * Sep 07, 2017
 */
public class SpringApplicationStarter extends AbstractStartStoppable {


    private SpringApplication application;
    private ConfigurableApplicationContext context;
    protected int port;
    protected int maxThreads = 200;

    public SpringApplicationStarter(Object resource, int port) {
        this(resource, port, 200);
    }

    public SpringApplicationStarter(Object resource, int port, int maxThreads) {
        application = new SpringApplication(resource);
        application.setBannerMode(Banner.Mode.OFF);
        this.port = port;
        this.maxThreads = maxThreads;
        application.setEnvironment(createEnvironment());
    }

    public int getPort() {
        return port;
    }

    @Override
    protected void doStart() throws Exception {
        context = application.run();
    }

    @Override
    protected void doStop() throws Exception {
        if (context != null) {
            context.close();
        }
    }

    protected ConfigurableEnvironment createEnvironment() {

        return new MyEnvironment();
    }

    class MyEnvironment extends StandardServletEnvironment {

        @Override
        protected void customizePropertySources(MutablePropertySources propertySources) {
            super.customizePropertySources(propertySources);

            Map<String, Object> properties = new HashMap<>();
            properties.put("server.port", String.valueOf(port));
            properties.put("server.tomcat.max-threads", String.valueOf(maxThreads));
            propertySources.addFirst(new MapPropertySource("TestAppServerProperty", properties));

        }
    }

}
