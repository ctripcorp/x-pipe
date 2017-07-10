package com.ctrip.xpipe.redis.console.migration.model.impl;

import com.ctrip.xpipe.api.codec.Codec;
import com.ctrip.xpipe.metric.HostPort;
import com.ctrip.xpipe.redis.console.AbstractConsoleTest;
import com.ctrip.xpipe.redis.console.migration.model.ShardMigrationResult;
import com.ctrip.xpipe.redis.console.migration.model.ShardMigrationResultStatus;
import com.ctrip.xpipe.redis.console.migration.model.ShardMigrationStep;
import org.junit.Assert;
import org.junit.Test;

/**
 * @author wenchao.meng
 *         <p>
 *         Jun 28, 2017
 */
public class DefaultShardMigrationResultTest extends AbstractConsoleTest {


    @Test
    public void testEncodeDecode(){

        DefaultShardMigrationResult result = new DefaultShardMigrationResult();
        for (ShardMigrationStep step : ShardMigrationStep.values()) {
            result.updateStepResult(step, true, randomString(10));
        }

        result.setNewMaster(new HostPort("127.0.0.1", randomPort()));

        String encode = result.encode();

        ShardMigrationResult decode = DefaultShardMigrationResult.fromEncodeStr(encode);

        logger.info("{}", decode);
        Assert.assertEquals(result, decode);

    }

    @Test
    public void testCodec() {

        DefaultShardMigrationResult result = new DefaultShardMigrationResult();

        for (ShardMigrationStep step : ShardMigrationStep.values()) {
            result.updateStepResult(step, true, randomString(10));
        }

        String encodeStr = Codec.DEFAULT.encode(result);
        logger.info("{}", encodeStr);

        DefaultShardMigrationResult decodeObj = Codec.DEFAULT.decode(encodeStr, DefaultShardMigrationResult.class);
        logger.info("{}", decodeObj);

        Assert.assertEquals(result, decodeObj);
    }

    @Test
    public void testOldDeserialPass() {

        String oldData = "{\"status\":\"SUCCESS\"," +
                "\"steps\":{" +
                "\"MIGRATE_OTHER_DC\":{\"true\":\"[info][2017-01-09 16:48:13.133][doChangeMetaCache]xpipe_test shard1 -> SHAJQ\\n[error][2017-01-09 16:48:13.136][changeSentinel]sentinelMeta not found:SHAOY xpipe_test shard1 0\\n[info][2017-01-09 16:48:13.165][chooseNewMaster]10.28.61.124:1234\\n[info][2017-01-09 16:48:13.166][makeKeepersOk][<?xml version=\\\"1.0\\\" encoding=\\\"utf-8\\\"?>\\r\\n<keeper id=\\\"7dce57c19767838298e86606302b595595cc4a52\\\" ip=\\\"10.15.138.233\\\" port=\\\"1234\\\" active=\\\"true\\\" keeperContainerId=\\\"17\\\"/>\\r\\n, <?xml version=\\\"1.0\\\" encoding=\\\"utf-8\\\"?>\\r\\n<keeper id=\\\"7dce57c19767838298e86606302b595595cc4a52\\\" ip=\\\"10.15.138.234\\\" port=\\\"1234\\\" active=\\\"false\\\" keeperContainerId=\\\"18\\\"/>\\r\\n]\\n[info][2017-01-09 16:48:13.183][makeKeepersOk]success\\n[info][2017-01-09 16:48:13.184][makeRedisesOk][<?xml version=\\\"1.0\\\" encoding=\\\"utf-8\\\"?>\\r\\n<redis id=\\\"unknown\\\" ip=\\\"10.15.138.234\\\" port=\\\"6479\\\" master=\\\"\\\"/>\\r\\n]->Pair[key=10.15.138.233, value=1234]\\n\"},\"MIGRATE_PREVIOUS_PRIMARY_DC\":{\"true\":\"Ignored : make previous primary dc read only\"}," +
                "\"MIGRATE\":{\"true\":\"Success\"}," +
                "\"CHECK\":{\"true\":\"Check success\"}," +
                "\"MIGRATE_NEW_PRIMARY_DC\":{\"true\":\"[info][2017-01-09 16:48:09.553][doChangeMetaCache]xpipe_test shard1 -> SHAJQ\\n[info][2017-01-09 16:48:09.554][chooseNewMaster][begin]\\n[info][2017-01-09 16:48:09.556][chooseNewMaster]10.28.61.125:6479\\n[info][2017-01-09 16:48:09.556][make redis master]Pair[key=10.28.61.125, value=6479]\\n[info][2017-01-09 16:48:09.562][make redis master]OK,OK\\n[info][2017-01-09 16:48:09.563][make slaves slaveof]Pair[key=10.28.61.125, value=6479],[]\\n[info][2017-01-09 16:48:09.563][make slaves slaveof]success\\n[info][2017-01-09 16:48:09.563][makeKeepersOk][<?xml version=\\\"1.0\\\" encoding=\\\"utf-8\\\"?>\\r\\n<keeper id=\\\"1f607d08f2781a8e11a78c9edbc6c2b7d4ecf7a3\\\" ip=\\\"10.28.61.124\\\" port=\\\"1234\\\" active=\\\"true\\\" keeperContainerId=\\\"15\\\"/>\\r\\n, <?xml version=\\\"1.0\\\" encoding=\\\"utf-8\\\"?>\\r\\n<keeper id=\\\"1f607d08f2781a8e11a78c9edbc6c2b7d4ecf7a3\\\" ip=\\\"10.28.61.125\\\" port=\\\"1234\\\" active=\\\"false\\\" keeperContainerId=\\\"13\\\"/>\\r\\n]\\n[info][2017-01-09 16:48:13.094][makeKeepersOk]success\\n[error][2017-01-09 16:48:13.094][addSentinel][fail]sentinelMeta not found:SHAJQ xpipe_test shard1 0\\n\"}}}";

        DefaultShardMigrationResult decodeObj = Codec.DEFAULT.decode(oldData, DefaultShardMigrationResult.class);

        logger.info("{}", decodeObj);
        Assert.assertEquals(ShardMigrationResultStatus.SUCCESS, decodeObj.getStatus());
        for(ShardMigrationStep step : ShardMigrationStep.values()){
            Assert.assertTrue(decodeObj.stepSuccess(step));
        }

    }
}
