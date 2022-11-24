package com.ctrip.xpipe.redis.keeper.store;

import com.ctrip.xpipe.gtid.GtidSet;
import com.ctrip.xpipe.netty.ByteBufUtils;
import com.ctrip.xpipe.redis.core.protocal.protocal.EofType;
import com.ctrip.xpipe.redis.core.store.RdbFileListener;
import com.ctrip.xpipe.redis.core.store.RdbStore;
import com.ctrip.xpipe.redis.core.store.RdbStoreListener;
import com.ctrip.xpipe.redis.core.store.ReplicationProgress;
import com.ctrip.xpipe.redis.core.store.GtidSetReplicationProgress;
import io.netty.buffer.ByteBuf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

/**
 * @author lishanglin
 * date 2022/6/6
 */
public class GtidRdbStore extends DefaultRdbStore implements RdbStore {

    private static final Logger logger = LoggerFactory.getLogger(GtidRdbStore.class);

    private AtomicReference<String> gtidSet = new AtomicReference<>();

    private final List<RdbFileListener> waitAuxListener;

    public GtidRdbStore(File file, long rdbOffset, EofType eofType, String gtidSet) throws IOException {
        super(file, rdbOffset, eofType);
        this.gtidSet.set(gtidSet);
        this.waitAuxListener = new ArrayList<>();
    }

    @Override
    public String getGtidSet() {
        return gtidSet.get();
    }

    @Override
    public int writeRdb(ByteBuf byteBuf) throws IOException {
        makeSureOpen();

        int wrote = ByteBufUtils.writeByteBufToFileChannel(byteBuf, channel);
        return wrote;
    }

    @Override
    public boolean updateRdbGtidSet(String gtidSet) {
        if (this.gtidSet.compareAndSet(null, gtidSet)) {
            notifyListenersRdbGtidSet(gtidSet);
            return true;
        }

        return false;
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
        rdbFileListener.setRdbFileInfo(eofType, new GtidSetReplicationProgress(new GtidSet(gtidSet.get())));
    }

    protected void notifyListenersRdbGtidSet(String rdbGtidSet) {

        for(RdbStoreListener listener : rdbStoreListeners){
            try{
                listener.onRdbGtidSet(rdbGtidSet);
            }catch(Throwable th){
                getLogger().error("[notifyListenersEndRdb]" + this, th);
            }
        }
    }

    @Override
    protected Logger getLogger() {
        return logger;
    }
}
