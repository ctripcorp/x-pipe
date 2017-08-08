package com.ctrip.xpipe.redis.console.controller.api.data.meta;

import com.ctrip.xpipe.codec.JsonCodec;
import com.ctrip.xpipe.redis.console.AbstractConsoleTest;
import com.ctrip.xpipe.redis.console.controller.api.migrate.meta.CheckPrepareRequest;
import org.junit.Assert;
import org.junit.Test;

/**
 * @author wenchao.meng
 *         <p>
 *         Aug 08, 2017
 */
public class CheckPrepareRequestTest extends AbstractConsoleTest{

    @Test
    public void testDerialOldVersion(){

        String json = "{ \"clusters\": [ \"cluster1\", \"cluster2\"], \"fromIdc\": \"SHAJQ\", \"isForce\": true }";

        CheckPrepareRequest request = JsonCodec.INSTANCE.decode(json, CheckPrepareRequest.class);
        Assert.assertEquals("SHAJQ", request.getFromIdc());
        Assert.assertEquals(null, request.getToIdc());
        Assert.assertEquals(2, request.getClusters().size());
        Assert.assertEquals(true, request.isForce());

    }

    @Test
    public void testDerialNewVersion(){

        String json = "{ \"clusters\": [ \"cluster1\", \"cluster2\"], \"fromIdc\": \"SHAJQ\", \"toIdc\" : \"SHAOY\", \"isForce\": true }";

        CheckPrepareRequest request = JsonCodec.INSTANCE.decode(json, CheckPrepareRequest.class);
        Assert.assertEquals("SHAJQ", request.getFromIdc());
        Assert.assertEquals("SHAOY", request.getToIdc());
        Assert.assertEquals(2, request.getClusters().size());
        Assert.assertEquals(true, request.isForce());

    }

}
