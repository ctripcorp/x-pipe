package com.ctrip.xpipe.redis.keeper.store.meta;

import com.ctrip.xpipe.AbstractTest;
import com.ctrip.xpipe.redis.core.meta.KeeperState;
import com.ctrip.xpipe.redis.core.protocal.protocal.EofMarkType;
import com.ctrip.xpipe.redis.core.protocal.protocal.EofType;
import com.ctrip.xpipe.redis.keeper.exception.replication.UnexpectedReplIdException;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;

import static org.junit.Assert.*;

/**
 * @author chen.zhu
 * <p>
 * May 19, 2020
 */
public class DefaultMetaStoreTest extends AbstractTest {


    @Test (expected = UnexpectedReplIdException.class)
    public void fixPsync0() throws IOException {

        DefaultMetaStore metaStore = new DefaultMetaStore(new File("/tmp/xpipe/test"), "20180118165046194-20180118165046194-294c90b4c9ed4d747a77b1b0f22ec28a8068013b");
        metaStore.becomeActive();
        metaStore.checkReplIdAndUpdateRdbInfo("rdb_1620671301121_e67222d2-eee1-48c4-bde7-5c6d37734ca4", new EofMarkType("94480e125b6ebb54dc7b9eae7b9c8ea00aeed56e"), 572767153, null);
    }
}