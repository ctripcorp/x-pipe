package com.ctrip.xpipe.redis.meta.server.rest.impl;

import com.ctrip.xpipe.redis.meta.server.rest.data.ResourceInfo;
import com.ctrip.xpipe.spring.AbstractController;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.web.embedded.tomcat.TomcatWebServer;
import org.springframework.boot.web.servlet.context.ServletWebServerApplicationContext;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import javax.annotation.Resource;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;

import static com.ctrip.xpipe.redis.meta.server.spring.MetaServerContextConfig.REPLICATION_ADJUST_EXECUTOR;

/**
 * @author lishanglin
 * date 2022/4/1
 */
@RestController
@RequestMapping(AbstractController.API_PREFIX)
public class ResourceController extends AbstractController {

    @Autowired
    private ServletWebServerApplicationContext context;

    @Resource(name = REPLICATION_ADJUST_EXECUTOR)
    private ExecutorService replAdjustExecutors;

    @GetMapping("/resource")
    public ResourceInfo getResource() {
        ThreadPoolExecutor httpThreadPoolExecutor = (ThreadPoolExecutor) ((TomcatWebServer) context.getWebServer())
                .getTomcat().getConnector().getProtocolHandler().getExecutor();

        ResourceInfo resourceInfo = new ResourceInfo();
        resourceInfo.collectDataFromHttpExecutor(httpThreadPoolExecutor);
        if (replAdjustExecutors instanceof ThreadPoolExecutor)
            resourceInfo.collectDataFromReplAdjustExecutor((ThreadPoolExecutor) replAdjustExecutors);

        return resourceInfo;
    }

}
