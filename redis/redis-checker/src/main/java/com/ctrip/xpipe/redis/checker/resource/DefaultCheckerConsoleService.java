package com.ctrip.xpipe.redis.checker.resource;

import com.ctrip.xpipe.redis.checker.CheckerConsoleService;
import com.ctrip.xpipe.redis.core.console.ConsoleCheckerPath;
import com.ctrip.xpipe.redis.core.entity.XpipeMeta;
import com.ctrip.xpipe.redis.core.service.AbstractService;
import org.springframework.context.annotation.Lazy;
import org.springframework.stereotype.Component;

/**
 * @author lishanglin
 * date 2021/3/16
 */
@Lazy
@Component
public class DefaultCheckerConsoleService extends AbstractService implements CheckerConsoleService {

    public XpipeMeta getXpipeMeta(String console, int clusterPartIndex) {
        return restTemplate.getForObject(console + ConsoleCheckerPath.PATH_GET_META,
                XpipeMeta.class, clusterPartIndex);
    }

}
