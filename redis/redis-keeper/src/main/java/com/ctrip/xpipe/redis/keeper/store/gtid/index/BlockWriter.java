package com.ctrip.xpipe.redis.keeper.store.gtid.index;

import com.ctrip.xpipe.api.utils.ControllableFile;
import com.ctrip.xpipe.utils.DefaultControllableFile;
import com.ctrip.xpipe.utils.StringUtil;

import java.io.IOException;
import java.nio.ByteBuffer;

public class BlockWriter implements AutoCloseable {

    private int cmdOffset;
    private int size;
    private ControllableFile controllableFile;
    private String currentUuid;
    private long currentGno;

    public static final int BLOCK_NAX_SIZE = 8 * 1024;

    private ByteBuffer blockCache;

    public BlockWriter(String currentUuid, long gno, int cmdOffset, String file,ByteBuffer blockCache) throws IOException {
        this(currentUuid,gno,cmdOffset,file);
        this.blockCache = blockCache;
    }

    public BlockWriter(String currentUuid, long gno, int cmdOffset, String file) throws IOException {
        this.cmdOffset = cmdOffset;
        this.currentUuid = currentUuid;
        this.currentGno = gno;
        this.size = 0;
        this.controllableFile = new DefaultControllableFile(file);
    }

    public boolean isGnoGap(String uuid, long gno) {
        return !StringUtil.trimEquals(this.currentUuid, uuid) || gno != currentGno + 1;
    }

    public void append(String uuid, long gno, int offset) throws IOException {
         int val = offset - cmdOffset;
         VarInt.encodeToByteBuffer(val,blockCache);

         cmdOffset = offset;
         this.currentGno = gno;
         this.currentUuid = uuid;
         this.size++;
    }

    public void flushBlock() throws IOException{
        if(blockCache != null && blockCache.hasRemaining()) {
            blockCache.flip();
            int bytesWritten = this.controllableFile.getFileChannel().write(blockCache);
            if (bytesWritten != blockCache.limit()) {
                throw new IOException("Failed to write the complete data to the file.");
            }
            blockCache.clear();
        }
    }

    public long getPosition() throws IOException {
        return this.controllableFile.getFileChannel().position();
    }

    public void finishWriter() throws IOException {
        this.close();
    }

    public int getSize() {
        return this.size;
    }

    public int getCmdOffset() {
        return this.cmdOffset;
    }

    @Override
    public void close() throws IOException {
        flushBlock();
        this.controllableFile.close();
    }

}
