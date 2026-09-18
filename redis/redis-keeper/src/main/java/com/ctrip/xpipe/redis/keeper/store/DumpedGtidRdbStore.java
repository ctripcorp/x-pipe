package com.ctrip.xpipe.redis.keeper.store;

import com.ctrip.xpipe.redis.core.protocal.protocal.EofType;
import com.ctrip.xpipe.redis.core.store.DumpedRdbStore;
import com.ctrip.xpipe.redis.core.store.ReplStage;
import com.ctrip.xpipe.redis.core.store.ReplId;
import com.ctrip.xpipe.redis.keeper.storage.AsyncFileSystem;

import java.io.File;
import java.io.IOException;
import java.util.function.IntSupplier;

/**
 * @author lishanglin
 * date 2022/6/6
 */
public class DumpedGtidRdbStore extends GtidRdbStore implements DumpedRdbStore {

    public DumpedGtidRdbStore(File file, AsyncFileSystem asyncFileSystem, IntSupplier asyncWriteMaxBytes,
                              ReplId fileSystemReplId) throws IOException {
        super(file, null, -1, null, null, null, null, null, asyncFileSystem, asyncWriteMaxBytes, fileSystemReplId);
    }

    @Override
    public EofType getEofType() {
        return this.eofType;
    }

    @Override
    public void setReplId(String replId) {
        this.replId = replId;
    }


    @Override
    public void setEofType(EofType eofType) {
        this.eofType = eofType;
    }

    @Override
    public void setRdbOffset(long rdbOffset){
        this.rdbOffset = rdbOffset;
    }

    @Override
    public void setReplProto(ReplStage.ReplProto replProto) {
        this.replProto = replProto;
    }

    @Override
    public void setMasterUuid(String masterUuid) {
        this.masterUuid = masterUuid;
    }

    @Override
    public void setGtidLost(String gtidLost) {
        this.gtidLost.set(gtidLost);
    }

}
