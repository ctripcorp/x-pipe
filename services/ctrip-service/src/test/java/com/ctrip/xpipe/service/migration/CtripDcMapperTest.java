package com.ctrip.xpipe.service.migration;

import com.ctrip.xpipe.service.AbstractServiceTest;
import com.ctrip.xpipe.api.migration.DcMapper;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

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


        CtripDcMapper ctripDcMapper = (CtripDcMapper) dcMapper;

        Map<String, String> rules = new HashMap<>();
        rules.put("NGXTH", "JQ");
        rules.put("FAT", "OY");

        Assert.assertEquals("NGXTH", ctripDcMapper.doReverse("JQ", rules));
        Assert.assertEquals("NGXTH", ctripDcMapper.doReverse("NGXTH", rules));


    }


}
