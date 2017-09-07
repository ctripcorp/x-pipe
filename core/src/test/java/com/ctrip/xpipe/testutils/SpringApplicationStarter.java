package com.ctrip.xpipe.testutils;

import com.ctrip.xpipe.lifecycle.AbstractStartStoppable;
import org.springframework.boot.Banner;
import org.springframework.boot.SpringApplication;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.core.env.ConfigurableEnvironment;
import org.springframework.core.env.MutablePropertySources;
import org.springframework.core.env.PropertySource;
import org.springframework.web.context.support.StandardServletEnvironment;

/**
 * @author wenchao.meng
 *         <p>
 *         Sep 07, 2017
 */
public class SpringApplicationStarter extends AbstractStartStoppable{


    private SpringApplication application;
    private ConfigurableApplicationContext context;
    private int port;

    public SpringApplicationStarter(Object resource, int port){
        application = new SpringApplication(resource);
        application.setBannerMode(Banner.Mode.OFF);
        application.setEnvironment(createEnvironment());
        this.port = port;
    }


    @Override
    protected void doStart() throws Exception {
        context = application.run();
    }

    @Override
    protected void doStop() throws Exception {
        if(context != null){
            context.close();
        }
    }

    private ConfigurableEnvironment createEnvironment() {

        return new MyEnvironment();
    }

    class MyEnvironment extends StandardServletEnvironment {

        @Override
        protected void customizePropertySources(MutablePropertySources propertySources) {
            super.customizePropertySources(propertySources);
            propertySources.addFirst(new PropertySource<Object>("TestAppServerProperty"){

                @Override
                public Object getProperty(String name) {

                    if(name.equals("server.port")){
                        return String.valueOf(port);
                    }
                    return null;
                }

            });
        }
    }

}
