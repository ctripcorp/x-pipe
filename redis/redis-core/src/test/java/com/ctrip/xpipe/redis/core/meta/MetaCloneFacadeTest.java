package com.ctrip.xpipe.redis.core.meta;

import com.ctrip.xpipe.redis.core.AbstractRedisTest;
import com.ctrip.xpipe.redis.core.entity.XpipeMeta;
import com.ctrip.xpipe.redis.core.meta.clone.MetaCloneFacade;
import org.junit.Assert;
import org.junit.Test;

/**
 * @author lishanglin
 * date 2023/12/13
 */
public class MetaCloneFacadeTest extends AbstractRedisTest {

    @Test
    public void testCloneXPipeMeta() {
        XpipeMeta xpipeMeta = getXpipeMeta();
        XpipeMeta clone = MetaCloneFacade.INSTANCE.clone(xpipeMeta);
        Assert.assertEquals(xpipeMeta.toString(), clone.toString());
    }

    @Override
    protected String getXpipeMetaConfigFile() {
        return "keeper.xml";
    }

}
