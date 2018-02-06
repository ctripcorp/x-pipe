package com.ctrip.xpipe.redis.core.meta;

import com.ctrip.xpipe.redis.core.AbstractRedisTest;
import com.ctrip.xpipe.redis.core.entity.DcMeta;
import com.ctrip.xpipe.redis.core.entity.XpipeMeta;
import org.junit.Assert;
import org.junit.Test;


/**
 * @author wenchao.meng
 *         <p>
 *         Aug 6, 2016
 */
public class MetaCloneTest extends AbstractRedisTest {


    @Test
    public void testClone() {

        XpipeMeta xpipeMeta = getXpipeMeta();

        DcMeta dcMeta = xpipeMeta.getDcs().values().iterator().next();

        DcMeta clone = MetaClone.clone(dcMeta);

        Assert.assertEquals(dcMeta, clone);

        clone.setId(randomString());
        Assert.assertNotEquals(dcMeta, clone);
    }

    @Test
    public void testCloneMultiTimes() {

        int count = 1 << 1;

        XpipeMeta xpipeMeta = getXpipeMeta();

        DcMeta dcMeta = xpipeMeta.getDcs().values().iterator().next();

        DcMeta clone = null;

        for (int i = 0; i < count; i++) {
            clone = MetaClone.clone(dcMeta);
        }

    }

    @Override
    protected String getXpipeMetaConfigFile() {
        return "keeper.xml";
    }

}
