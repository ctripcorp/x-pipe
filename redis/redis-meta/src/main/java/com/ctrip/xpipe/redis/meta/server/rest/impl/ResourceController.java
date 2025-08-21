package com.ctrip.xpipe.redis.meta.server.rest.impl;

import com.ctrip.xpipe.redis.meta.server.rest.data.ResourceInfo;
import com.ctrip.xpipe.spring.AbstractController;
import jakarta.annotation.Resource;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;

import static com.ctrip.xpipe.redis.meta.server.spring.MetaServerContextConfig.REPLICATION_ADJUST_EXECUTOR;
import static com.ctrip.xpipe.spring.AbstractSpringConfigContext.tryGetTomcatHttpExecutor;

/**
 * @author lishanglin
 * date 2022/4/1
 */
@RestController
@RequestMapping(AbstractController.API_PREFIX)
public class ResourceController extends AbstractController {

    @Resource(name = REPLICATION_ADJUST_EXECUTOR)
    private ExecutorService replAdjustExecutors;

    @GetMapping("/resource")
    public ResourceInfo getResource() {
        ResourceInfo resourceInfo = new ResourceInfo();

        Executor tomcatHttpExecutor = tryGetTomcatHttpExecutor();
        if (tomcatHttpExecutor instanceof ThreadPoolExecutor)
            resourceInfo.collectDataFromHttpExecutor((ThreadPoolExecutor) tomcatHttpExecutor);

        if (replAdjustExecutors instanceof ThreadPoolExecutor)
            resourceInfo.collectDataFromReplAdjustExecutor((ThreadPoolExecutor) replAdjustExecutors);

        return resourceInfo;
    }

}
