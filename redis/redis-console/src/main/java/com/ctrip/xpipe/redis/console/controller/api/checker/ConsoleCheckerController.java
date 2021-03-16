package com.ctrip.xpipe.redis.console.controller.api.checker;

import com.ctrip.xpipe.redis.console.checker.CheckerManager;
import com.ctrip.xpipe.redis.console.controller.AbstractConsoleController;
import com.ctrip.xpipe.redis.console.spring.condition.ConsoleServerMode;
import com.ctrip.xpipe.redis.console.spring.condition.ConsoleServerModeCondition;
import com.ctrip.xpipe.redis.core.console.ConsoleCheckerPath;
import com.ctrip.xpipe.redis.core.entity.XpipeMeta;
import com.ctrip.xpipe.redis.core.meta.MetaCache;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

/**
 * @author lishanglin
 * date 2021/3/16
 */
@RestController
@ConsoleServerMode(ConsoleServerModeCondition.SERVER_MODE.CONSOLE)
public class ConsoleCheckerController extends AbstractConsoleController {

    @Autowired
    private MetaCache metaCache;

    @Autowired
    private CheckerManager checkerManager;

    @GetMapping(ConsoleCheckerPath.PATH_GET_META)
    public XpipeMeta getDividedMeta(@PathVariable int index) {
        if (index < 0) throw new IllegalArgumentException("illegal index " + index);
        return metaCache.getDividedXpipeMeta(index);
    }

    @GetMapping(ConsoleCheckerPath.PATH_GET_PROXY_CHAINS)
    public void getProxyTunnels() {
        // update checker status
    }

    @PutMapping(ConsoleCheckerPath.PATH_PUT_HEALTH_CHECK_RESULT)
    public void reportHealthCheckResult() {

    }

}
