package com.ctrip.xpipe.redis.keeper.store;

import com.ctrip.xpipe.redis.core.protocal.protocal.EofType;
import com.ctrip.xpipe.redis.core.store.DumpedRdbStore;

import java.io.File;
import java.io.IOException;

/**
 * @author lishanglin
 * date 2022/6/6
 */
public class DumpedGtidRdbStore extends GtidRdbStore implements DumpedRdbStore {

    public DumpedGtidRdbStore(File file) throws IOException {
        super(file, -1, null);
    }

    @Override
    public EofType getEofType() {
        return this.eofType;
    }


    @Override
    public void setEofType(EofType eofType) {
        this.eofType = eofType;
    }

    @Override
    public File getRdbFile() {
        return file;
    }

    @Override
    public void setRdbOffset(long rdbOffset){
        this.rdbOffset = rdbOffset;
    }

}
