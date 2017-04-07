package com.ctrip.xpipe.service.migration;

import com.ctrip.xpipe.AbstractServiceTest;
import com.ctrip.xpipe.api.migration.DcMapper;
import org.junit.Assert;
import org.junit.Test;

/**
 * @author wenchao.meng
 *         <p>
 *         Apr 07, 2017
 */
public class CtripDcMapperTest extends AbstractServiceTest{

    @Test
    public void testDcMapper(){

        DcMapper dcMapper = DcMapper.INSTANCE;
        Assert.assertTrue(dcMapper instanceof CtripDcMapper);
        Assert.assertNotEquals("NGXNH", dcMapper.getDc("NTGXH"));
        String randomDc = randomString(10);
        Assert.assertEquals(randomDc, dcMapper.getDc(randomDc));


    }


}
