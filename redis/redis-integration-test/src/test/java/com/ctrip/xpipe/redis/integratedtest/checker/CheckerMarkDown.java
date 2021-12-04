package com.ctrip.xpipe.redis.integratedtest.checker;

import com.ctrip.xpipe.redis.core.entity.RedisMeta;
import com.ctrip.xpipe.redis.integratedtest.AbstractIntegratedTest;
import org.junit.Test;

import java.util.List;

/**
 * @author Slight
 * <p>
 * Dec 04, 2021 11:51 AM
 */
public class CheckerMarkDown extends AbstractIntegratedTest {

    protected String activeDc = "jq";

    @Test
    public void markdownDeadOnewayRedisInBackupIdc() {

    }

    @Override
    protected List<RedisMeta> getRedisSlaves() {
        return null;
    }
}
