package com.ctrip.xpipe.redis.keeper.store;

import com.ctrip.xpipe.gtid.GtidSet;
import com.ctrip.xpipe.redis.core.protocal.protocal.EofType;
import com.ctrip.xpipe.redis.core.store.GtidSetReplicationProgress;
import com.ctrip.xpipe.redis.core.store.RdbFileListener;
import com.ctrip.xpipe.redis.core.store.RdbStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicReference;

/**
 * @author lishanglin
 * date 2022/6/6
 */
public class GtidRdbStore extends DefaultRdbStore implements RdbStore {

    private static final Logger logger = LoggerFactory.getLogger(GtidRdbStore.class);

    private AtomicReference<String> gtidSet = new AtomicReference<>();

    public GtidRdbStore(File file, String replId, long rdbOffset, EofType eofType, String gtidSet) throws IOException {
        super(file, replId, rdbOffset, eofType);
        this.gtidSet.set(gtidSet);
    }

    @Override
    public String getGtidSet() {
        return gtidSet.get();
    }

    @Override
    public boolean isGtidSetInit() {
        return getGtidSet() != null;
    }

    @Override
    public boolean supportGtidSet() {
        return isGtidSetInit() && !GtidSet.EMPTY_GTIDSET.equals(getGtidSet());
    }

    @Override
    public boolean updateRdbGtidSet(String gtidSet) {
        return this.gtidSet.compareAndSet(null, gtidSet);
    }

    @Override
    protected void doReadRdbFileInfo(RdbFileListener rdbFileListener) {
        if (!rdbFileListener.supportProgress(GtidSetReplicationProgress.class)) {
            super.doReadRdbFileInfo(rdbFileListener);
            return;
        }

        if (null == gtidSet) {
            throw new IllegalStateException("rdb.gtidset null");
        }
        rdbFileListener.setRdbFileInfo(eofType, new GtidSetReplicationProgress(new GtidSet(gtidSet.get()), rdbOffset));
    }

    @Override
    protected Logger getLogger() {
        return logger;
    }
}
